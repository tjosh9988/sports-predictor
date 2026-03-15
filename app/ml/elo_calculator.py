"""
elo_calculator.py — Elo rating system for all Bet Hero sports.

Key design decisions
--------------------
- K-factor varies by context: higher for international/finals, lower for low-tier leagues
- Home advantage: +HOME_ADVANTAGE points added to expected score for home team
- Mean reversion: ratings regress towards 1500 at the start of each new season
  (prevents runaway ratings from old data dominating predictions)
- No data leakage: Elo at time T only uses results from before T
- Batch storage: writes all historical snapshots to elo_ratings table in one batch
- Point-in-time lookup: get_elo_at_date(team_id, date) is O(log n) via bisect

Elo formula
-----------
  E_home = 1 / (1 + 10^((R_away + HA - R_home) / 400))
  E_away = 1 - E_home
  R_home_new = R_home + K * (S_home - E_home)
  R_away_new = R_away + K * (S_away - E_away)

Where:
  S = 1 (win), 0.5 (draw), 0 (loss)
  HA = home advantage constant (sport-dependent)
  K  = learning rate (context-dependent)
"""

from __future__ import annotations

import bisect
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

# ─────────────────────────── Elo Constants ─────────────────────────────────

STARTING_RATING  = 1500.0
SCALING_FACTOR   = 400.0      # standard Elo divisor
MEAN_REVERSION   = 0.3        # fraction of gap from 1500 to revert at season start
                               # new_rating = old + MEAN_REVERSION * (1500 - old)

# Home advantage (in Elo points) — tuned per sport
HOME_ADVANTAGE: dict[str, float] = {
    "football":   65.0,
    "basketball": 35.0,   # NBA shorter travel, but crowd effect
    "tennis":     0.0,    # neutral venues at majors; used 0 as baseline
    "nfl":        45.0,
    "cricket":    30.0,
    "nhl":        15.0,
    "mlb":        25.0,
    "default":    40.0,
}

# K-factor definitions: higher = faster adaptation
# Mapped from a priority string on the match's league/round/importance
K_FACTOR_MAP: dict[str, float] = {
    "world_cup_final":   80.0,
    "world_cup":         60.0,
    "continental_final": 60.0,
    "continental":       50.0,
    "domestic_cup_final":50.0,
    "domestic_cup":      35.0,
    "tier1":             30.0,   # Top domestic league (EPL, La Liga, NBA, etc.)
    "tier2":             25.0,
    "tier3":             20.0,
    "friendly":          10.0,
    "default":           25.0,
}

# League-tier overrides (league name fragments → K-factor key)
LEAGUE_TIER_MAP: list[tuple[str, str]] = [
    # International
    ("world cup",         "world_cup"),
    ("champions league",  "continental"),
    ("europa league",     "continental"),
    ("nations league",    "continental"),
    ("copa libertadores", "continental"),
    ("afc champions",     "continental"),
    # Top domestic
    ("premier league",    "tier1"),
    ("la liga",           "tier1"),
    ("bundesliga",        "tier1"),
    ("serie a",           "tier1"),
    ("ligue 1",           "tier1"),
    ("primeira liga",     "tier1"),
    ("eredivisie",        "tier1"),
    ("nba",               "tier1"),
    ("nfl",               "tier1"),
    ("mlb",               "tier1"),
    ("nhl",               "tier1"),
    ("ipl",               "tier1"),
    ("grand slam",        "domestic_cup_final"),
    ("masters 1000",      "tier1"),
    # Tier 2
    ("championship",      "tier2"),
    ("2. bundesliga",     "tier2"),
    ("serie b",           "tier2"),
    ("ligue 2",           "tier2"),
    # Cups
    ("fa cup",            "domestic_cup"),
    ("copa del rey",      "domestic_cup"),
    ("dfb-pokal",         "domestic_cup"),
    ("coppa italia",      "domestic_cup"),
]


def _k_factor(league_name: str, round_name: str | None = None) -> float:
    """Resolve K-factor from league name and round string."""
    combined = f"{(league_name or '').lower()} {(round_name or '').lower()}"
    if "final" in combined:
        if "world" in combined:
            return K_FACTOR_MAP["world_cup_final"]
        if "champions" in combined or "europa" in combined:
            return K_FACTOR_MAP["continental_final"]
        return K_FACTOR_MAP["domestic_cup_final"]
    for fragment, tier in LEAGUE_TIER_MAP:
        if fragment in combined:
            return K_FACTOR_MAP.get(tier, K_FACTOR_MAP["default"])
    return K_FACTOR_MAP["default"]


def _home_advantage(sport_slug: str) -> float:
    return HOME_ADVANTAGE.get(sport_slug, HOME_ADVANTAGE["default"])


# ─────────────────────────── Core Elo Engine ───────────────────────────────

@dataclass
class EloSnapshot:
    """One stored Elo rating snapshot for a team at a point in time."""
    team_id:      int
    rating:       float
    match_id:     int | None
    calculated_at: datetime


@dataclass
class EloState:
    """Live Elo state for a single team."""
    team_id:    int
    rating:     float = STARTING_RATING
    season:     str   = ""
    # Parallel sorted lists for O(log n) point-in-time lookup
    timestamps: list[datetime]  = field(default_factory=list)
    ratings:    list[float]     = field(default_factory=list)

    def record(self, at: datetime) -> None:
        self.timestamps.append(at)
        self.ratings.append(self.rating)

    def rating_at(self, at: datetime) -> float:
        """Binary-search for the rating at or before *at*."""
        idx = bisect.bisect_right(self.timestamps, at) - 1
        if idx < 0:
            return STARTING_RATING
        return self.ratings[idx]


class EloCalculator:
    """
    Recomputes Elo ratings for all teams from scratch using the full
    match history stored in Supabase, then persists every snapshot.

    Usage
    -----
        calc = EloCalculator(supabase_admin)
        calc.run(sport_slug="football")   # or "nba", "nfl", etc.
        rating = calc.get_elo_at_date(team_id=42, at=date(2023, 8, 12))
    """

    BATCH_SIZE = 500

    def __init__(self, supabase_client, sport_slug: str):
        self.client     = supabase_client
        self.sport_slug = sport_slug
        self._states:   dict[int, EloState] = {}          # team_id → EloState
        self._snapshots: list[EloSnapshot]  = []
        self._sport_id: int | None          = None

    # ── Public API ──────────────────────────────────────────────

    def run(self) -> int:
        """
        Full pipeline:
          1. Load all finished matches for the sport (ordered by match_date ASC)
          2. For each match: update Elo, apply mean reversion on season change
          3. Persist all snapshots to elo_ratings table
        Returns number of snapshots stored.
        """
        logger.info("[Elo/%s] Starting calculation …", self.sport_slug)
        self._sport_id = self._fetch_sport_id()
        if not self._sport_id:
            logger.error("[Elo/%s] Sport not found in DB", self.sport_slug)
            return 0

        matches = self._load_matches()
        logger.info("[Elo/%s] Processing %d matches …", self.sport_slug, len(matches))

        for match in matches:
            self._process_match(match)

        self._flush_snapshots()
        logger.info("[Elo/%s] Done — %d snapshots persisted", self.sport_slug, len(self._snapshots))
        return len(self._snapshots)

    def get_elo_at_date(self, team_id: int, at: date | datetime) -> float:
        """
        Returns the Elo rating for *team_id* at the given date/datetime.
        Only valid after calling run() (uses in-memory EloState).
        For DB-backed lookup use get_elo_at_date_from_db().
        """
        if isinstance(at, date) and not isinstance(at, datetime):
            at = datetime(at.year, at.month, at.day)
        state = self._states.get(team_id)
        if not state:
            return STARTING_RATING
        return state.rating_at(at)

    def get_elo_at_date_from_db(self, team_id: int, at: date | datetime) -> float:
        """
        DB-backed point-in-time Elo lookup.
        Use this in the ML feature pipeline (no in-memory state required).
        """
        if isinstance(at, date) and not isinstance(at, datetime):
            at = datetime(at.year, at.month, at.day)
        res = (
            self.client.table("elo_ratings")
            .select("rating")
            .eq("team_id", team_id)
            .lte("calculated_at", at.isoformat())
            .order("calculated_at", desc=True)
            .limit(1)
            .execute()
        )
        if res.data:
            return res.data[0]["rating"]
        return STARTING_RATING

    # ── Match processing ────────────────────────────────────────

    def _process_match(self, match: dict) -> None:
        home_id    = match["home_team_id"]
        away_id    = match["away_team_id"]
        home_score = match.get("home_score", 0) or 0
        away_score = match.get("away_score", 0) or 0
        match_id   = match["id"]
        league     = match.get("league_name", "")
        round_name = match.get("round")
        season     = match.get("season", "")
        match_date = self._parse_dt(match.get("match_date", ""))

        if match_date is None:
            return

        home_state = self._get_or_create(home_id)
        away_state = self._get_or_create(away_id)

        # Mean reversion on new season
        self._apply_mean_reversion(home_state, season)
        self._apply_mean_reversion(away_state, season)

        # Score coefficients (handles draws)
        if home_score > away_score:
            s_home, s_away = 1.0, 0.0
        elif away_score > home_score:
            s_home, s_away = 0.0, 1.0
        else:
            s_home, s_away = 0.5, 0.5

        # Expected scores
        ha = _home_advantage(self.sport_slug)
        e_home = 1.0 / (1.0 + 10 ** ((away_state.rating - home_state.rating - ha) / SCALING_FACTOR))
        e_away = 1.0 - e_home

        # Goal-ratio multiplier (rewards dominant wins slightly more)
        gd_mult = self._goal_difference_multiplier(home_score, away_score)

        # K-factor
        k = _k_factor(league, round_name)

        # Update
        home_state.rating += gd_mult * k * (s_home - e_home)
        away_state.rating += gd_mult * k * (s_away - e_away)

        # Clamp to sane range
        home_state.rating = max(200.0, min(3000.0, home_state.rating))
        away_state.rating = max(200.0, min(3000.0, away_state.rating))

        # Record snapshots
        home_state.record(match_date)
        away_state.record(match_date)

        self._snapshots.append(EloSnapshot(home_id, home_state.rating, match_id, match_date))
        self._snapshots.append(EloSnapshot(away_id, away_state.rating, match_id, match_date))

    @staticmethod
    def _goal_difference_multiplier(home: int, away: int) -> float:
        """
        FiveThirtyEight-style goal-difference multiplier.
        Dampened log scale: a 5-0 win counts ~1.7× a 1-0 win.
        """
        gd = abs(home - away)
        if gd == 0:
            return 1.0
        return 1.0 + 0.3 * (gd - 1) ** 0.7

    @staticmethod
    def _apply_mean_reversion(state: EloState, new_season: str) -> None:
        """Regress rating toward 1500 when the season changes."""
        if new_season and new_season != state.season and state.season != "":
            gap = STARTING_RATING - state.rating
            state.rating += MEAN_REVERSION * gap
        state.season = new_season

    def _get_or_create(self, team_id: int) -> EloState:
        if team_id not in self._states:
            self._states[team_id] = EloState(team_id=team_id)
        return self._states[team_id]

    # ── DB I/O ──────────────────────────────────────────────────

    def _fetch_sport_id(self) -> int | None:
        res = (
            self.client.table("sports")
            .select("id")
            .eq("slug", self.sport_slug)
            .single()
            .execute()
        )
        return res.data["id"] if res.data else None

    def _load_matches(self) -> list[dict]:
        """
        Load all finished matches with league name joined.
        Ordered by match_date ASC so we process history chronologically.
        Pagination: 10 000 rows per page.
        """
        all_matches: list[dict] = []
        page_size   = 10_000
        offset      = 0

        while True:
            res = (
                self.client.table("matches")
                .select(
                    "id, home_team_id, away_team_id, home_score, away_score,"
                    " match_date, season, round,"
                    " leagues!inner(name)"
                )
                .eq("sport_id", self._sport_id)
                .eq("status", "finished")
                .not_.is_("home_score", "null")
                .not_.is_("away_score", "null")
                .order("match_date", desc=False)
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = res.data or []
            for m in batch:
                league_info = m.pop("leagues", {}) or {}
                m["league_name"] = league_info.get("name", "")
            all_matches.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        return all_matches

    def _flush_snapshots(self) -> None:
        """Write all collected EloSnapshots to Supabase in one batched upsert."""
        if not self._snapshots:
            return
        rows = [
            {
                "team_id":      s.team_id,
                "rating":       round(s.rating, 4),
                "match_id":     s.match_id,
                "calculated_at": s.calculated_at.isoformat(),
            }
            for s in self._snapshots
        ]
        total = len(rows)
        for start in range(0, total, self.BATCH_SIZE):
            batch = rows[start : start + self.BATCH_SIZE]
            try:
                self.client.table("elo_ratings").upsert(
                    batch, on_conflict="team_id,calculated_at"
                ).execute()
            except Exception as exc:
                logger.error(
                    "[Elo/%s] Batch flush failed (rows %d-%d): %s",
                    self.sport_slug, start, start + len(batch), exc,
                )
        logger.info("[Elo/%s] Flushed %d snapshots", self.sport_slug, total)

    @staticmethod
    def _parse_dt(value: str) -> datetime | None:
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value[:19], fmt[:len(fmt)])
            except (ValueError, TypeError):
                continue
        return None

    # ── Current ratings summary ─────────────────────────────────

    def current_ratings(self) -> pd.DataFrame:
        """
        Returns a DataFrame of all teams' current Elo ratings,
        useful for inspecting state after run().
        """
        rows = [
            {
                "team_id":    s.team_id,
                "elo_rating": round(s.rating, 1),
                "snapshots":  len(s.timestamps),
            }
            for s in self._states.values()
        ]
        df = pd.DataFrame(rows).sort_values("elo_rating", ascending=False).reset_index(drop=True)
        return df

    def update_teams_table(self) -> None:
        """Sync the final Elo rating back to teams.elo_rating column."""
        rows = [
            {"id": tid, "elo_rating": round(state.rating, 4)}
            for tid, state in self._states.items()
        ]
        for start in range(0, len(rows), self.BATCH_SIZE):
            batch = rows[start : start + self.BATCH_SIZE]
            try:
                self.client.table("teams").upsert(batch, on_conflict="id").execute()
            except Exception as exc:
                logger.error("[Elo/%s] update_teams_table batch failed: %s", self.sport_slug, exc)
        logger.info("[Elo/%s] teams.elo_rating updated for %d teams", self.sport_slug, len(rows))


# ─────────────────────────── Convenience Functions ─────────────────────────

def run_elo_for_all_sports(supabase_client) -> dict[str, int]:
    """
    Convenience: run Elo calculation for all 7 sports.
    Returns {sport_slug: snapshots_stored}.
    """
    from ..ingestion.run_importers import SPORT_ORDER
    # Deduplicate tennis Tours into one slug
    slugs = []
    for s in SPORT_ORDER:
        slug = s.replace("tennis_atp", "tennis").replace("tennis_wta", "tennis")
        if slug not in slugs:
            slugs.append(slug)

    results: dict[str, int] = {}
    for slug in slugs:
        try:
            calc = EloCalculator(supabase_client, slug)
            n = calc.run()
            calc.update_teams_table()
            results[slug] = n
        except Exception as exc:
            logger.error("[Elo/%s] Failed: %s", slug, exc, exc_info=True)
            results[slug] = -1
    return results


def get_elo_at_date(
    supabase_client,
    team_id: int,
    at: date | datetime,
) -> float:
    """
    Stateless DB-backed point-in-time Elo lookup.
    Safe to call from any context (ML pipeline, API endpoint, etc.).
    No EloCalculator instance required.
    """
    if isinstance(at, date) and not isinstance(at, datetime):
        at = datetime(at.year, at.month, at.day)
    res = (
        supabase_client.table("elo_ratings")
        .select("rating")
        .eq("team_id", team_id)
        .lte("calculated_at", at.isoformat())
        .order("calculated_at", desc=True)
        .limit(1)
        .execute()
    )
    if res.data:
        return res.data[0]["rating"]
    return STARTING_RATING


# ─────────────────────────── CLI Entry Point ───────────────────────────────

if __name__ == "__main__":
    import sys
    import argparse

    logging.basicConfig(level=logging.INFO)

    p = argparse.ArgumentParser(description="Compute Elo ratings for all sports")
    p.add_argument("--sport", default=None,
                   help="Single sport slug (omit to run all)")
    p.add_argument("--update-teams", action="store_true",
                   help="Also sync final ratings back to teams.elo_rating")
    args = p.parse_args()

    from ..database import get_supabase_admin
    client = get_supabase_admin()

    if args.sport:
        calc = EloCalculator(client, args.sport)
        n = calc.run()
        if args.update_teams:
            calc.update_teams_table()
        print(f"\n✅ {args.sport}: {n} snapshots stored")
        print(calc.current_ratings().head(20).to_string(index=False))
    else:
        results = run_elo_for_all_sports(client)
        print("\n📊 Elo Summary:")
        for sport, count in results.items():
            status = f"{count:,d} snapshots" if count >= 0 else "FAILED"
            print(f"  {sport:<16} {status}")
    sys.exit(0)
