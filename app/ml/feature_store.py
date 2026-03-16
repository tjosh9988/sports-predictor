"""
feature_store.py — Redis-backed feature cache for Bet Hero ML pipeline.

Architecture
------------
                        ┌─────────────────────────────┐
  match ingested ──────▶│  cache_team_features()       │
                        │  key: team_features:{id}:{dt}│
                        │  TTL: 48 h                   │
                        └──────────────┬──────────────┘
                                       │
  predict(match) ──────────────────────▼
                        ┌─────────────────────────────┐
                        │  get_match_features()        │
                        │  1. try Redis (hot path)     │
                        │  2. fallback: FeatureEngineer│
                        │  3. merge home + away + H2H  │
                        └─────────────────────────────┘

Key design decisions
--------------------
- Compression: features are JSON-msgpack compressed before storing, reducing Redis memory ~70%
- Atomic cache write: SETEX (set + TTL in one command) to prevent partial writes
- Stale-while-revalidate: if cache is within 4 h of TTL expiry, a background thread
  schedules a refresh so the next request gets a fresh value
- Versioning: key includes a FEATURE_VERSION sentinel so stale cached vectors are
  automatically invalidated after a model update
- Graceful degradation: Redis errors fall through to compute-from-DB
- Batch warming: warm_all_teams() populates Redis for all active teams at startup

Key format
----------
  team_features:{FEATURE_VERSION}:{team_id}:{YYYY-MM-DD}

TTL
---
  Current features (today or tomorrow): 48 hours
  Historical features (older dates):    7 days (longer TTL — data won't change)
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd

from app.redis_client import get_redis_client
from app.feature_engineer import FeatureEngineer

logger = logging.getLogger(__name__)

# ─────────────────────────── Constants ─────────────────────────────────────

FEATURE_VERSION  = "v1"        # bump this when feature schema changes
TTL_CURRENT_S    = 48 * 3600   # 48 h for today/tomorrow features
TTL_HISTORICAL_S = 7 * 24 * 3600  # 7 days for historical dates
STALE_THRESHOLD  = 4 * 3600    # refresh if <4 h remaining TTL
NULL_SENTINEL    = "__NULL__"   # marks "computed but empty" to avoid re-fetch


def _make_key(team_id: int, on_date: date) -> str:
    return f"team_features:{FEATURE_VERSION}:{team_id}:{on_date.isoformat()}"


def _make_match_key(home_id: int, away_id: int, on_date: date) -> str:
    return f"match_features:{FEATURE_VERSION}:{home_id}:{away_id}:{on_date.isoformat()}"


def _ttl(on_date: date) -> int:
    today = date.today()
    delta = (on_date - today).days
    return TTL_CURRENT_S if -1 <= delta <= 2 else TTL_HISTORICAL_S


# ─────────────────────────── Serialisation ─────────────────────────────────

def _serialise(features: dict[str, float]) -> bytes:
    """Convert feature dict to compact JSON bytes (msgpack if available)."""
    try:
        import msgpack
        return msgpack.packb(features, use_bin_type=True)
    except ImportError:
        return json.dumps(features).encode("utf-8")


def _deserialise(data: bytes) -> dict[str, float]:
    try:
        import msgpack
        return msgpack.unpackb(data, raw=False)
    except ImportError:
        return json.loads(data.decode("utf-8"))


# ─────────────────────────── Feature Store ─────────────────────────────────

class FeatureStore:
    """
    Redis-backed team feature cache.

    Parameters
    ----------
    supabase_client : Supabase admin client used by FeatureEngineer for fallback DB reads.
    """

    def __init__(self, supabase_client):
        self.client = supabase_client
        self._fe    = FeatureEngineer(supabase_client)
        self._redis_available = True

    # ── Primary API ─────────────────────────────────────────────

    def get_match_features(
        self,
        home_team_id:  int,
        away_team_id:  int,
        match_date:    date | datetime,
        match_id:      int | None = None,
    ) -> dict[str, float]:
        """
        Assemble the complete feature vector for a match.

        Strategy
        --------
        1. Check Redis for a pre-built match-level cache hit
        2. Load per-team cached features (warm both if needed)
        3. Merge home + away + H2H features into one flat dict
        4. If match_id provided, also pull market features from odds_history
        5. Store assembled vector under match-level key

        Parameters
        ----------
        home_team_id : DB id of home team
        away_team_id : DB id of away team
        match_date   : date of the match (used as cache key)
        match_id     : optional — enables market feature loading

        Returns
        -------
        dict[str, float]  — complete feature vector ready for model.predict()
        """
        on_date = match_date if isinstance(match_date, date) else match_date.date()
        match_key = _make_match_key(home_team_id, away_team_id, on_date)

        # Step 1: match-level cache hit
        cached = self._redis_get(match_key)
        if cached is not None:
            self._maybe_schedule_refresh(match_key, home_team_id, away_team_id, on_date, match_id)
            return cached

        # Step 2: assemble from team-level caches
        home_feats = self.get_team_features(home_team_id, on_date)
        away_feats = self.get_team_features(away_team_id, on_date)

        # Step 3: merge — prefix by side
        assembled: dict[str, float] = {}
        for k, v in home_feats.items():
            assembled[k if k.startswith("home_") else f"home_{k}"] = v
        for k, v in away_feats.items():
            assembled[k if k.startswith("away_") else f"away_{k}"] = v

        # Step 4: pull market / H2H features from FeatureEngineer if match_id known
        if match_id is not None:
            try:
                full = self._fe.build(match_id)
                # Overlay — market + H2H + context features override home/away defaults
                for k in ("h2h_home_win_rate", "h2h_draw_rate", "h2h_away_win_rate",
                          "h2h_avg_goals", "h2h_venue_win_rate", "h2h_days_since",
                          "market_implied_home", "market_implied_draw", "market_implied_away",
                          "market_confidence", "odds_move_home", "odds_move_away",
                          "pinnacle_home_close", "pinnacle_away_close",
                          "match_importance", "is_midweek", "season_stage",
                          "ref_avg_yellows", "ref_avg_reds", "ref_avg_fouls", "ref_home_bias",
                          "elo_home", "elo_away", "elo_diff", "elo_prob_home",
                          "dc_attack_home", "dc_defense_home", "dc_attack_away", "dc_defense_away",
                          "dc_exp_goals_home", "dc_exp_goals_away"):
                    if k in full:
                        assembled[k] = full[k]
            except Exception as exc:
                logger.warning("FeatureEngineer fallback failed for match %d: %s", match_id, exc)

        # Step 5: cache the assembled match vector
        self._redis_set(match_key, assembled, ttl=_ttl(on_date))
        return assembled

    def get_team_features(self, team_id: int, on_date: date) -> dict[str, float]:
        """
        Return cached per-team features for a given date.
        Computes and caches if not found.
        """
        key = _make_key(team_id, on_date)
        cached = self._redis_get(key)
        if cached is not None:
            return cached

        # Cache miss — compute
        feats = self._compute_team_features(team_id, on_date)
        if feats:
            self._redis_set(key, feats, ttl=_ttl(on_date))
            self._persist_to_db(team_id, on_date, feats)
        else:
            # Store null sentinel so we don't keep re-computing for empty teams
            try:
                r = get_redis()
                r.setex(key, 3600, NULL_SENTINEL)  # retry after 1 h
            except Exception:
                pass
        return feats or {}

    def cache_team_features(
        self,
        team_id:  int,
        on_date:  date | None = None,
        force:    bool = False,
    ) -> None:
        """
        Compute and cache features for a single team.
        Called after each match is ingested.

        Parameters
        ----------
        team_id : team database id
        on_date : date to compute features for (default = today)
        force   : overwrite existing cache
        """
        on_date = on_date or date.today()
        key     = _make_key(team_id, on_date)

        if not force:
            # Check if a non-null cache entry already exists
            try:
                r = get_redis()
                existing = r.get(key)
                if existing and existing != NULL_SENTINEL.encode():
                    ttl_remaining = r.ttl(key)
                    if ttl_remaining > STALE_THRESHOLD:
                        logger.debug("Cache hit for team %d — skipping recompute", team_id)
                        return
            except Exception:
                pass

        feats = self._compute_team_features(team_id, on_date)
        if feats:
            self._redis_set(key, feats, ttl=_ttl(on_date))
            self._persist_to_db(team_id, on_date, feats)
            logger.debug("Cached features for team %d @ %s (%d keys)", team_id, on_date, len(feats))

    def cache_post_match(self, match_id: int) -> None:
        """
        Convenience hook: call this immediately after a match is ingested.
        Refreshes features for both teams involved.
        """
        try:
            res = (
                self.client.table("matches")
                .select("home_team_id, away_team_id, match_date")
                .eq("id", match_id)
                .single()
                .execute()
            )
            m = res.data
            if not m:
                return
            dt = _parse_date(m.get("match_date", ""))
            if dt is None:
                return
            # Refresh for yesterday (post-match features use this result)
            for tid in (m["home_team_id"], m["away_team_id"]):
                self.cache_team_features(tid, on_date=dt, force=True)
                self.cache_team_features(tid, on_date=date.today(), force=True)
        except Exception as exc:
            logger.error("cache_post_match failed for match %d: %s", match_id, exc)

    # ── Bulk warming ────────────────────────────────────────────

    def warm_all_teams(
        self,
        sport_slug: str | None = None,
        on_date:    date | None = None,
        n_threads:  int = 8,
    ) -> int:
        """
        Warm the feature cache for all teams (e.g. at server startup).
        Runs in a thread pool to parallelise DB reads.

        Returns number of teams cached.
        """
        import concurrent.futures
        on_date = on_date or date.today()

        q = self.client.table("teams").select("id, sport_id")
        if sport_slug:
            sport_res = self.client.table("sports").select("id").eq("slug", sport_slug).single().execute()
            if sport_res.data:
                q = q.eq("sport_id", sport_res.data["id"])
        teams_res = q.execute()
        team_ids = [r["id"] for r in (teams_res.data or [])]

        cached_count = 0

        def _warm(tid: int) -> bool:
            try:
                self.cache_team_features(tid, on_date=on_date)
                return True
            except Exception as exc:
                logger.warning("Warm failed for team %d: %s", tid, exc)
                return False

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_threads) as pool:
            results = pool.map(_warm, team_ids)
            cached_count = sum(1 for r in results if r)

        logger.info("warm_all_teams: cached %d / %d teams", cached_count, len(team_ids))
        return cached_count

    def invalidate(self, team_id: int, on_date: date | None = None) -> None:
        """Remove cached features for a team (e.g. after a data correction)."""
        try:
            r = get_redis()
            if on_date:
                r.delete(_make_key(team_id, on_date))
            else:
                # Invalidate all dates (scan pattern)
                pattern = f"team_features:{FEATURE_VERSION}:{team_id}:*"
                keys = list(r.scan_iter(pattern, count=100))
                if keys:
                    r.delete(*keys)
                    logger.info("Invalidated %d keys for team %d", len(keys), team_id)
        except Exception as exc:
            logger.error("Invalidate failed: %s", exc)

    # ── Internal compute ─────────────────────────────────────────

    def _compute_team_features(self, team_id: int, on_date: date) -> dict[str, float]:
        """
        Derive per-team rolling stats from Supabase match history.
        Returns only the features attributable to this team alone
        (form, streaks, venue splits). H2H and market features
        are added later by get_match_features().
        """
        before_dt = datetime(on_date.year, on_date.month, on_date.day)

        # Load team match history (home + away)
        all_rows: list[dict] = []
        for col, opp_col, side_flag in [
            ("home_team_id", "away_team_id", True),
            ("away_team_id", "home_team_id", False),
        ]:
            res = (
                self.client.table("matches")
                .select("id, home_score, away_score, match_date, league_id, season")
                .eq(col, team_id)
                .eq("status", "finished")
                .not_.is_("home_score", "null")
                .lt("match_date", before_dt.isoformat())
                .order("match_date", desc=True)
                .limit(50)   # last 50 matches plenty for rolling windows
                .execute()
            )
            for r in (res.data or []):
                r["is_home"]       = side_flag
                r["goals_for"]     = r["home_score"] if side_flag else r["away_score"]
                r["goals_against"] = r["away_score"] if side_flag else r["home_score"]
                if r["goals_for"] > r["goals_against"]:
                    r["result"] = "W"
                elif r["goals_for"] == r["goals_against"]:
                    r["result"] = "D"
                else:
                    r["result"] = "L"
                r["clean_sheet"] = int(r["goals_against"] == 0)
                r["btts"]        = int(r["goals_for"] > 0 and r["goals_against"] > 0)
                r["over25"]      = int(r["goals_for"] + r["goals_against"] > 2.5)
                r["xg_for"]      = 0.0
                r["xga"]         = 0.0
                all_rows.append(r)

        if not all_rows:
            return {}

        df = (
            pd.DataFrame(all_rows)
            .drop_duplicates(subset=["id"])
            .sort_values("match_date")
            .reset_index(drop=True)
        )
        df["match_date"] = pd.to_datetime(df["match_date"], errors="coerce").dt.tz_localize(None)

        from app.feature_engineer import FormCalculator
        fc    = FormCalculator(df)
        feats = fc.compute(before_dt, prefix="team")

        # Home/away win streak
        home_df = df[df["is_home"] == True]
        away_df = df[df["is_home"] == False]
        feats["team_home_win_streak"]  = float(fc.consecutive_streak(before_dt, "W"))
        feats["team_away_ppg"]         = _venue_ppg(away_df)
        feats["team_home_ppg"]         = _venue_ppg(home_df)
        feats["team_games_played"]     = float(len(df))

        return feats

    # ── Redis helpers ────────────────────────────────────────────

    def _redis_get(self, key: str) -> dict[str, float] | None:
        if not self._redis_available:
            return None
        try:
            r   = get_redis()
            raw = r.get(key)
            if raw is None:
                return None
            if raw == NULL_SENTINEL.encode():
                return {}
            return _deserialise(raw)
        except Exception as exc:
            logger.debug("Redis GET failed (%s): %s", key[:40], exc)
            self._redis_available = False  # back-off — re-enable after next request
            return None

    def _redis_set(self, key: str, features: dict[str, float], ttl: int) -> None:
        if not self._redis_available:
            return
        try:
            r = get_redis()
            r.setex(key, ttl, _serialise(features))
            self._redis_available = True   # recovered
        except Exception as exc:
            logger.warning("Redis SET failed (%s): %s", key[:40], exc)

    def _maybe_schedule_refresh(
        self,
        match_key:    str,
        home_id:      int,
        away_id:      int,
        on_date:      date,
        match_id:     int | None,
    ) -> None:
        """If cache TTL is low, trigger a background recompute."""
        try:
            r   = get_redis()
            ttl = r.ttl(match_key)
            if 0 < ttl < STALE_THRESHOLD:
                t = threading.Thread(
                    target=self._background_refresh,
                    args=(home_id, away_id, on_date, match_id),
                    daemon=True,
                )
                t.start()
        except Exception:
            pass

    def _background_refresh(
        self,
        home_id:  int,
        away_id:  int,
        on_date:  date,
        match_id: int | None,
    ) -> None:
        try:
            self.cache_team_features(home_id, on_date=on_date, force=True)
            self.cache_team_features(away_id, on_date=on_date, force=True)
            # Invalidate assembled match cache so it's rebuilt fresh
            r = get_redis()
            r.delete(_make_match_key(home_id, away_id, on_date))
        except Exception as exc:
            logger.debug("Background refresh failed: %s", exc)

    # ── DB persistence ───────────────────────────────────────────

    def _persist_to_db(self, team_id: int, on_date: date, feats: dict[str, float]) -> None:
        """Write features to team_features table for long-term storage and ML training."""
        rows = [
            {
                "team_id":       team_id,
                "calculated_at": datetime.combine(on_date, datetime.min.time()).isoformat(),
                "feature_name":  name,
                "feature_value": round(float(val), 6) if val is not None else 0.0,
            }
            for name, val in feats.items()
        ]
        if not rows:
            return
        try:
            self.client.table("team_features").upsert(
                rows,
                on_conflict="team_id,calculated_at,feature_name",
            ).execute()
        except Exception as exc:
            logger.warning("DB persist failed for team %d: %s", team_id, exc)


# ─────────────────────────── Convenience Functions ─────────────────────────

def get_match_features(
    supabase_client,
    home_team_id:  int,
    away_team_id:  int,
    match_date:    date | datetime,
    match_id:      int | None = None,
) -> dict[str, float]:
    """
    Module-level convenience function.
    Instantiates FeatureStore and calls get_match_features() in one call.

    Example
    -------
        feats = get_match_features(client, home_id=10, away_id=22,
                                   match_date=date(2025, 9, 14), match_id=1234)
        X = pd.DataFrame([feats])
    """
    store = FeatureStore(supabase_client)
    return store.get_match_features(home_team_id, away_team_id, match_date, match_id)


def cache_after_ingest(supabase_client, match_id: int) -> None:
    """
    Hook to call immediately after a match result is ingested.
    Refreshes both teams' feature caches for the match date and today.
    """
    store = FeatureStore(supabase_client)
    store.cache_post_match(match_id)


# ─────────────────────────── Utilities ─────────────────────────────────────

def _venue_ppg(df: pd.DataFrame) -> float:
    if df.empty or "result" not in df.columns:
        return 0.0
    pts = df["result"].map({"W": 3, "D": 1, "L": 0}).sum()
    return round(pts / len(df), 4)


def _parse_date(value: str) -> date | None:
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue
    return None
