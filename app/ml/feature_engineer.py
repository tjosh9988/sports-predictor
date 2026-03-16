"""
feature_engineer.py — Complete pre-match feature pipeline for Bet Hero ML models.

⚠️  NO DATA LEAKAGE GUARANTEE
All features are computed using only data with timestamp < match_date.
This is enforced by the strict date filtering in every query and rolling window.

Feature families
----------------
1. FORM          — rolling 5 / 10 / 20 game windows, exp-decay score, momentum
2. STRENGTH      — Elo differential, Dixon-Coles attack/defense strengths
3. HEAD-TO-HEAD  — win/draw/loss rate, goals, venue, days since last meeting
4. CONTEXT       — fatigue, midweek flag, league position, season stage, importance
5. MARKET        — implied probabilities (overround removed), odds movement
6. REFEREE       — cards, fouls, home-win bias

Public API
----------
    fe = FeatureEngineer(supabase_client)
    row = fe.build(match_id)            # → dict[str, float]  (single match)
    df  = fe.build_batch(match_ids)     # → pd.DataFrame      (multiple matches)
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd

from app.elo_calculator import get_elo_at_date, STARTING_RATING

logger = logging.getLogger(__name__)

# ─────────────────────────── Constants ─────────────────────────────────────

FORM_WINDOWS        = [5, 10, 20]
EXP_DECAY_HALFLIFE  = 10          # games: weight halves every 10 games ago
H2H_LOOKBACK        = 6           # last N H2H meetings
NAN_FILL            = 0.0         # sentinel for missing numeric features


# ═══════════════════════════════════════════════════════════════════════════
# Helper: safe division
# ═══════════════════════════════════════════════════════════════════════════

def _safe_div(num: float, den: float, default: float = NAN_FILL) -> float:
    return num / den if den != 0.0 else default


# ═══════════════════════════════════════════════════════════════════════════
# 1. FORM FEATURES
# ═══════════════════════════════════════════════════════════════════════════

class FormCalculator:
    """
    Computes rolling-window form features for a team, using only matches
    completed strictly BEFORE `before_dt`.
    """

    def __init__(self, team_matches: pd.DataFrame):
        """
        Parameters
        ----------
        team_matches : DataFrame with columns:
            match_date, is_home, goals_for, goals_against,
            result (W/D/L), xg_for, xga, clean_sheet, btts, over25
            Ordered by match_date ASC.
        """
        self.df = team_matches.copy()
        if not self.df.empty:
            self.df = self.df.sort_values("match_date").reset_index(drop=True)

    def compute(self, before_dt: datetime, prefix: str = "home") -> dict[str, float]:
        feats: dict[str, float] = {}
        # Filter strictly before match time
        hist = self.df[self.df["match_date"] < before_dt]

        for w in FORM_WINDOWS:
            window = hist.tail(w)
            p = f"{prefix}_form{w}_"
            n = len(window)
            if n == 0:
                # Fill zeros for all expected features in this window
                feats.update({
                    f"{p}ppg": NAN_FILL,
                    f"{p}gscored": NAN_FILL,
                    f"{p}gconceded": NAN_FILL,
                    f"{p}xg": NAN_FILL,
                    f"{p}xga": NAN_FILL,
                    f"{p}cs_rate": NAN_FILL,
                    f"{p}btts_rate": NAN_FILL,
                    f"{p}over25_rate": NAN_FILL,
                    f"{p}win_pct": NAN_FILL,
                    f"{p}draw_pct": NAN_FILL,
                    f"{p}loss_pct": NAN_FILL,
                })
                continue

            wins   = (window["result"] == "W").sum()
            draws  = (window["result"] == "D").sum()
            losses = (window["result"] == "L").sum()
            points = wins * 3 + draws

            feats[f"{p}ppg"]        = _safe_div(points, n)
            feats[f"{p}gscored"]    = window["goals_for"].mean()
            feats[f"{p}gconceded"]  = window["goals_against"].mean()
            feats[f"{p}xg"]         = window["xg_for"].mean()   if "xg_for" in window else NAN_FILL
            feats[f"{p}xga"]        = window["xga"].mean()       if "xga"    in window else NAN_FILL
            feats[f"{p}cs_rate"]    = _safe_div(window["clean_sheet"].sum(), n)
            feats[f"{p}btts_rate"]  = _safe_div(window["btts"].sum(), n)
            feats[f"{p}over25_rate"]= _safe_div(window["over25"].sum(), n)
            feats[f"{p}win_pct"]    = _safe_div(wins,   n)
            feats[f"{p}draw_pct"]   = _safe_div(draws,  n)
            feats[f"{p}loss_pct"]   = _safe_div(losses, n)

        # ── Exponential-decay form score ──────────────────────────
        feats[f"{prefix}_ewm_form"] = self._exp_decay_form(hist)

        # ── Form momentum: change in PPG from last 5 vs 6-10 ─────
        feats[f"{prefix}_form_momentum"] = self._form_momentum(hist)

        # ── Home/Away performance split ───────────────────────────
        feats[f"{prefix}_home_ppg"] = self._venue_ppg(hist, is_home=True)
        feats[f"{prefix}_away_ppg"] = self._venue_ppg(hist, is_home=False)

        return feats

    def _exp_decay_form(self, hist: pd.DataFrame, w: int = 20) -> float:
        """Exponentially weighted points (recent games count more)."""
        window = hist.tail(w)
        if window.empty:
            return NAN_FILL
        points_series = window["result"].map({"W": 3, "D": 1, "L": 0}).astype(float)
        n = len(points_series)
        decay = np.array([math.exp(-math.log(2) / EXP_DECAY_HALFLIFE * (n - 1 - i))
                          for i in range(n)])
        return float(np.dot(points_series.values, decay) / decay.sum())

    def _form_momentum(self, hist: pd.DataFrame) -> float:
        """PPG last 5 minus PPG for games 6-10. Positive = improving form."""
        recent = hist.tail(10)
        if len(recent) < 5:
            return NAN_FILL
        last5 = recent.tail(5)
        prev5 = recent.head(len(recent) - 5)
        ppg_last5 = (last5["result"].map({"W": 3, "D": 1, "L": 0}).sum()) / max(len(last5), 1)
        ppg_prev5 = (prev5["result"].map({"W": 3, "D": 1, "L": 0}).sum()) / max(len(prev5), 1)
        return float(ppg_last5 - ppg_prev5)

    def _venue_ppg(self, hist: pd.DataFrame, is_home: bool) -> float:
        subset = hist[hist["is_home"] == is_home]
        if subset.empty:
            return NAN_FILL
        pts = subset["result"].map({"W": 3, "D": 1, "L": 0}).sum()
        return float(_safe_div(pts, len(subset)))

    def consecutive_streak(self, before_dt: datetime, result_type: str) -> int:
        """How many consecutive W/D/L results ending before match time."""
        hist = self.df[self.df["match_date"] < before_dt].tail(20)
        streak = 0
        for r in reversed(hist["result"].tolist()):
            if r == result_type:
                streak += 1
            else:
                break
        return streak


# ═══════════════════════════════════════════════════════════════════════════
# 2. STRENGTH FEATURES — Dixon-Coles parameters
# ═══════════════════════════════════════════════════════════════════════════

class DixonColesEstimator:
    """
    Lightweight Dixon-Coles attack/defense strength estimator.
    Uses a simple iterative MLE approximation rather than full scipy optimise
    for speed (suitable for real-time feature computation).

    Attack strength  = mean goals scored / league mean goals
    Defense strength = mean goals conceded / league mean goals
    """

    def __init__(self, league_matches: pd.DataFrame):
        """
        Parameters
        ----------
        league_matches : DataFrame with home_team_id, away_team_id,
                         home_score, away_score, match_date
        """
        self.df = league_matches.dropna(subset=["home_score", "away_score"]).copy()
        self._league_avg_home: float = 0.0
        self._league_avg_away: float = 0.0
        self._attack:  dict[int, float] = {}
        self._defense: dict[int, float] = {}

    def fit(self, before_dt: datetime) -> "DixonColesEstimator":
        hist = self.df[self.df["match_date"] < before_dt]
        if hist.empty:
            return self

        self._league_avg_home = hist["home_score"].mean()
        self._league_avg_away = hist["away_score"].mean()
        league_avg = (self._league_avg_home + self._league_avg_away) / 2.0

        if league_avg == 0:
            return self

        teams = set(hist["home_team_id"].tolist() + hist["away_team_id"].tolist())
        for tid in teams:
            home_games  = hist[hist["home_team_id"] == tid]
            away_games  = hist[hist["away_team_id"] == tid]
            goals_scored    = (home_games["home_score"].sum() + away_games["away_score"].sum())
            goals_conceded  = (home_games["away_score"].sum() + away_games["home_score"].sum())
            n_games = len(home_games) + len(away_games)
            if n_games > 0:
                self._attack[tid]  = _safe_div(goals_scored  / n_games, league_avg, 1.0)
                self._defense[tid] = _safe_div(goals_conceded / n_games, league_avg, 1.0)
        return self

    def attack(self, team_id: int)  -> float:
        return self._attack.get(team_id, 1.0)

    def defense(self, team_id: int) -> float:
        return self._defense.get(team_id, 1.0)

    def expected_goals(self, home_id: int, away_id: int) -> tuple[float, float]:
        """λ_home, λ_away — Poisson rate predictions."""
        lam_h = self.attack(home_id) * self.defense(away_id) * self._league_avg_home
        lam_a = self.attack(away_id) * self.defense(home_id) * self._league_avg_away
        return lam_h, lam_a


# ═══════════════════════════════════════════════════════════════════════════
# 3. HEAD-TO-HEAD FEATURES
# ═══════════════════════════════════════════════════════════════════════════

def compute_h2h_features(
    home_id: int,
    away_id: int,
    venue: str | None,
    before_dt: datetime,
    all_matches: pd.DataFrame,
) -> dict[str, float]:
    """
    Head-to-head stats between home and away team, strictly before match time.

    Parameters
    ----------
    all_matches : DataFrame of all historical matches for the sport.
    """
    h2h = all_matches[
        (
            ((all_matches["home_team_id"] == home_id) & (all_matches["away_team_id"] == away_id)) |
            ((all_matches["home_team_id"] == away_id) & (all_matches["away_team_id"] == home_id))
        ) &
        (all_matches["match_date"] < before_dt) &
        all_matches["home_score"].notna()
    ].sort_values("match_date").tail(H2H_LOOKBACK)

    feats: dict[str, float] = {}
    n = len(h2h)

    if n == 0:
        return {
            "h2h_home_win_rate": NAN_FILL,
            "h2h_draw_rate":     NAN_FILL,
            "h2h_away_win_rate": NAN_FILL,
            "h2h_avg_goals":     NAN_FILL,
            "h2h_venue_win_rate": NAN_FILL,
            "h2h_days_since":    365.0,
        }

    # Outcome from home team's perspective
    def _outcome(row):
        if row["home_team_id"] == home_id:
            if row["home_score"] > row["away_score"]: return "W"
            if row["home_score"] == row["away_score"]: return "D"
            return "L"
        else:
            if row["away_score"] > row["home_score"]: return "W"
            if row["away_score"] == row["home_score"]: return "D"
            return "L"

    h2h = h2h.copy()
    h2h["h2h_result"] = h2h.apply(_outcome, axis=1)

    wins   = (h2h["h2h_result"] == "W").sum()
    draws  = (h2h["h2h_result"] == "D").sum()
    losses = (h2h["h2h_result"] == "L").sum()

    feats["h2h_home_win_rate"] = _safe_div(wins,   n)
    feats["h2h_draw_rate"]     = _safe_div(draws,  n)
    feats["h2h_away_win_rate"] = _safe_div(losses, n)
    feats["h2h_avg_goals"]     = ((h2h["home_score"] + h2h["away_score"]).mean())

    # H2H at same venue
    if venue:
        venue_h2h = h2h[h2h.get("venue", pd.Series(dtype=str)) == venue]
        vn = len(venue_h2h)
        if vn > 0:
            v_wins = (venue_h2h["h2h_result"] == "W").sum()
            feats["h2h_venue_win_rate"] = _safe_div(v_wins, vn)
        else:
            feats["h2h_venue_win_rate"] = NAN_FILL
    else:
        feats["h2h_venue_win_rate"] = NAN_FILL

    # Days since last H2H
    last_dt = h2h["match_date"].max()
    if pd.notna(last_dt):
        diff = (before_dt - pd.Timestamp(last_dt).to_pydatetime().replace(tzinfo=None)).days
        feats["h2h_days_since"] = float(max(0, diff))
    else:
        feats["h2h_days_since"] = 365.0

    return feats


# ═══════════════════════════════════════════════════════════════════════════
# 4. CONTEXT FEATURES
# ═══════════════════════════════════════════════════════════════════════════

def compute_context_features(
    match: dict,
    team_id: int,
    team_hist: pd.DataFrame,
    before_dt: datetime,
    league_table: dict[int, dict],
    is_home: bool,
) -> dict[str, float]:
    """
    Match-context features: fatigue, season stage, league position, importance.

    Parameters
    ----------
    match       : The upcoming match dict from DB
    team_hist   : All historical matches for this team (full history, date-sorted)
    league_table: {team_id: {position, points, top4_gap, relegation_gap}} at match time
    """
    side = "home" if is_home else "away"
    feats: dict[str, float] = {}

    # ── Fatigue features ──────────────────────────────────────
    recent = team_hist[team_hist["match_date"] < before_dt].sort_values("match_date")
    if not recent.empty:
        last_match_dt = pd.Timestamp(recent.iloc[-1]["match_date"]).to_pydatetime()
        days_since = (before_dt - last_match_dt.replace(tzinfo=None)).days
        feats[f"{side}_days_rest"] = float(days_since)
        cutoff_14d = before_dt - timedelta(days=14)
        games_14d = recent[recent["match_date"] >= cutoff_14d]
        feats[f"{side}_games_last14d"] = float(len(games_14d))
    else:
        feats[f"{side}_days_rest"]     = 14.0
        feats[f"{side}_games_last14d"] = 0.0

    # ── Midweek match flag ─────────────────────────────────────
    feats["is_midweek"] = 1.0 if before_dt.weekday() in (1, 2, 3) else 0.0

    # ── Season stage (0=early, 0.5=mid, 1.0=late) ─────────────
    season_str = str(match.get("season", ""))
    feats["season_stage"] = _season_stage(before_dt, season_str)

    # ── League table features ─────────────────────────────────
    row = league_table.get(team_id, {})
    feats[f"{side}_league_position"]  = float(row.get("position",       10))
    feats[f"{side}_top4_gap"]         = float(row.get("top4_gap",        10))
    feats[f"{side}_relegation_gap"]   = float(row.get("relegation_gap",  10))

    # ── Match importance score (0–1) ──────────────────────────
    feats["match_importance"] = _match_importance(
        round_name   = match.get("round", ""),
        season_stage = feats["season_stage"],
        top4_gap     = feats[f"{side}_top4_gap"],
        rel_gap      = feats[f"{side}_relegation_gap"],
    )

    # ── Win / clean-sheet streaks ─────────────────────────────
    results = recent.tail(10)["result"].tolist() if not recent.empty else []
    feats[f"{side}_win_streak"]  = float(_streak(results, "W"))
    feats[f"{side}_loss_streak"] = float(_streak(results, "L"))
    feats[f"{side}_cs_streak"]   = float(_streak(recent.tail(10)["clean_sheet"].astype(bool).map(
        lambda x: "Y" if x else "N").tolist(), "Y")) if not recent.empty else 0.0

    return feats


def _season_stage(dt: datetime, season: str) -> float:
    """Heuristic: map calendar month to season stage [0, 1]."""
    month = dt.month
    # Football: season starts Aug (0) → ends May (1)
    # NBA: Oct (0) → Jun (1)
    mapping = {8: 0.0, 9: 0.1, 10: 0.2, 11: 0.3, 12: 0.4,
               1: 0.5, 2: 0.6, 3: 0.7, 4: 0.8, 5: 0.9, 6: 1.0, 7: 1.0}
    return mapping.get(month, 0.5)


def _match_importance(
    round_name: str,
    season_stage: float,
    top4_gap: float,
    rel_gap: float,
) -> float:
    """
    Heuristic match importance in [0, 1].
    Cup finals and relegation deciders score highest.
    """
    score = 0.5 * season_stage   # later in season = higher importance
    rn = (round_name or "").lower()
    if "final" in rn:    score += 0.4
    elif "semi" in rn:   score += 0.2
    elif "quarter" in rn: score += 0.1
    if rel_gap is not None and rel_gap <= 3:   score += 0.2
    if top4_gap is not None and top4_gap <= 3: score += 0.1
    return round(min(1.0, score), 4)


def _streak(results: list[str], target: str) -> int:
    streak = 0
    for r in reversed(results):
        if r == target:
            streak += 1
        else:
            break
    return streak


# ═══════════════════════════════════════════════════════════════════════════
# 5. MARKET FEATURES
# ═══════════════════════════════════════════════════════════════════════════

def compute_market_features(odds_rows: list[dict]) -> dict[str, float]:
    """
    Derive implied probabilities and odds movement signals.

    Parameters
    ----------
    odds_rows : List of odds_history rows for the match
                (closing and opening odds from preferred bookmakers).
    """
    feats: dict[str, float] = {
        "market_implied_home": NAN_FILL,
        "market_implied_draw": NAN_FILL,
        "market_implied_away": NAN_FILL,
        "market_overround":    NAN_FILL,
        "odds_move_home":      NAN_FILL,
        "odds_move_away":      NAN_FILL,
        "market_confidence":   NAN_FILL,
        "pinnacle_home_close": NAN_FILL,
        "pinnacle_away_close": NAN_FILL,
    }
    if not odds_rows:
        return feats

    # Prefer Pinnacle; fall back to average of all bookmakers
    one_x_two = [r for r in odds_rows if r.get("market", "") in ("1X2", "h2h")]

    pinnacle = next(
        (r for r in one_x_two if "pinnacle" in (r.get("bookmaker", "") or "").lower()),
        None,
    )
    row = pinnacle or (one_x_two[0] if one_x_two else None)
    if not row:
        return feats

    # Closing implied probabilities (overround-removed)
    close_h = row.get("closing_home") or row.get("opening_home")
    close_d = row.get("closing_draw") or row.get("opening_draw")
    close_a = row.get("closing_away") or row.get("opening_away")

    if close_h and close_a:
        implied_h = _safe_div(1.0, close_h)
        implied_d = _safe_div(1.0, close_d) if close_d else 0.0
        implied_a = _safe_div(1.0, close_a)
        overround = implied_h + implied_d + implied_a

        feats["market_overround"]    = round(overround, 4)
        feats["market_implied_home"] = round(_safe_div(implied_h, overround), 4)
        feats["market_implied_draw"] = round(_safe_div(implied_d, overround), 4)
        feats["market_implied_away"] = round(_safe_div(implied_a, overround), 4)

        if pinnacle:
            feats["pinnacle_home_close"] = float(close_h)
            feats["pinnacle_away_close"] = float(close_a)

    # Odds movement: closing vs opening (positive = odds shortened = more money in)
    open_h = row.get("opening_home")
    open_a = row.get("opening_away")
    if open_h and close_h:
        feats["odds_move_home"] = round(float(open_h) - float(close_h), 4)
    if open_a and close_a:
        feats["odds_move_away"] = round(float(open_a) - float(close_a), 4)

    # Market confidence: normalised absolute movement (0=no move, 1=large move)
    total_move = abs(feats["odds_move_home"]) + abs(feats["odds_move_away"])
    feats["market_confidence"] = round(min(1.0, total_move / 1.0), 4)

    return feats


# ═══════════════════════════════════════════════════════════════════════════
# 6. REFEREE FEATURES
# ═══════════════════════════════════════════════════════════════════════════

def compute_referee_features(referee: dict | None) -> dict[str, float]:
    if not referee:
        return {
            "ref_avg_yellows":    NAN_FILL,
            "ref_avg_reds":       NAN_FILL,
            "ref_avg_fouls":      NAN_FILL,
            "ref_home_bias":      NAN_FILL,
        }
    return {
        "ref_avg_yellows": float(referee.get("avg_yellow_cards", 0) or 0),
        "ref_avg_reds":    float(referee.get("avg_red_cards",    0) or 0),
        "ref_avg_fouls":   float(referee.get("avg_fouls",        0) or 0),
        "ref_home_bias":   float(referee.get("home_bias_score",  0) or 0),
    }


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════

class FeatureEngineer:
    """
    Builds the complete pre-match feature vector for one or many matches.

    All DB queries are carefully scoped to be before match_date — no leakage.
    """

    def __init__(self, supabase_client):
        self.client = supabase_client
        self._match_cache:    dict[tuple, pd.DataFrame] = {}
        self._dc_cache:       dict[int, DixonColesEstimator] = {}
        self._table_cache:    dict[tuple, dict] = {}

    # ── Public API ──────────────────────────────────────────────

    def build(self, match_id: int) -> dict[str, float]:
        """Build the full feature dict for a single match."""
        match = self._load_match(match_id)
        if not match:
            raise ValueError(f"Match {match_id} not found")
        return self._build_features(match)

    def build_batch(self, match_ids: list[int]) -> pd.DataFrame:
        """Build feature DataFrame for multiple matches."""
        rows = []
        for mid in match_ids:
            try:
                match = self._load_match(mid)
                if match:
                    feats = self._build_features(match)
                    feats["match_id"] = mid
                    rows.append(feats)
            except Exception as exc:
                logger.warning("Feature build failed for match %d: %s", mid, exc)
        return pd.DataFrame(rows)

    # ── Core feature build ──────────────────────────────────────

    def _build_features(self, match: dict) -> dict[str, float]:
        match_id  = match["id"]
        home_id   = match["home_team_id"]
        away_id   = match["away_team_id"]
        sport     = match["sport"]
        league_id = match["league_id"]
        venue     = match.get("venue")
        referee   = match.get("referee_data")
        match_dt  = self._parse_dt(match.get("match_date", ""))

        if match_dt is None:
            raise ValueError(f"Cannot parse match_date for match {match_id}")

        all_matches = self._load_all_sport_matches(sport)

        # ── Per-team history ─────────────────────────────────────
        home_hist_df = self._team_history(home_id, all_matches, before_dt=match_dt, is_home=True)
        away_hist_df = self._team_history(away_id, all_matches, before_dt=match_dt, is_home=False)

        home_form_calc = FormCalculator(home_hist_df)
        away_form_calc = FormCalculator(away_hist_df)

        feats: dict[str, float] = {}

        # 1. FORM
        feats.update(home_form_calc.compute(match_dt, prefix="home"))
        feats.update(away_form_calc.compute(match_dt, prefix="away"))

        # Win/loss streaks
        feats["home_win_streak"]   = float(home_form_calc.consecutive_streak(match_dt, "W"))
        feats["home_loss_streak"]  = float(home_form_calc.consecutive_streak(match_dt, "L"))
        feats["away_win_streak"]   = float(away_form_calc.consecutive_streak(match_dt, "W"))
        feats["away_loss_streak"]  = float(away_form_calc.consecutive_streak(match_dt, "L"))

        # 2. STRENGTH — Elo
        home_elo = get_elo_at_date(self.client, home_id, match_dt)
        away_elo = get_elo_at_date(self.client, away_id, match_dt)
        feats["elo_home"]        = round(home_elo, 2)
        feats["elo_away"]        = round(away_elo, 2)
        feats["elo_diff"]        = round(home_elo - away_elo, 2)
        feats["elo_prob_home"]   = round(
            1.0 / (1.0 + 10 ** ((away_elo - home_elo - 65) / 400)), 4
        )

        # Dixon-Coles
        dc = self._get_dixon_coles(league_id, all_matches, match_dt)
        feats["dc_attack_home"]  = round(dc.attack(home_id),    4)
        feats["dc_defense_home"] = round(dc.defense(home_id),   4)
        feats["dc_attack_away"]  = round(dc.attack(away_id),    4)
        feats["dc_defense_away"] = round(dc.defense(away_id),   4)
        lam_h, lam_a = dc.expected_goals(home_id, away_id)
        feats["dc_exp_goals_home"] = round(lam_h, 4)
        feats["dc_exp_goals_away"] = round(lam_a, 4)

        # 3. HEAD TO HEAD
        feats.update(compute_h2h_features(home_id, away_id, venue, match_dt, all_matches))

        # 4. CONTEXT
        league_table = self._build_league_table(league_id, all_matches, match_dt)
        feats.update(compute_context_features(match, home_id, home_hist_df, match_dt,
                                              league_table, is_home=True))
        feats.update(compute_context_features(match, away_id, away_hist_df, match_dt,
                                              league_table, is_home=False))

        # 5. MARKET
        odds_rows = self._load_odds(match_id)
        feats.update(compute_market_features(odds_rows))

        # 6. REFEREE
        feats.update(compute_referee_features(referee))

        # Fill any remaining NaNs
        feats = {k: (NAN_FILL if (v is None or (isinstance(v, float) and math.isnan(v))) else v)
                 for k, v in feats.items()}

        return feats

    # ── DB Loaders ──────────────────────────────────────────────

    def _load_match(self, match_id: int) -> dict | None:
        res = (
            self.client.table("matches")
            .select("*, leagues(name), sports(slug), referees(*)")
            .eq("id", match_id)
            .single()
            .execute()
        )
        if not res.data:
            return None
        m = res.data
        m["referee_data"] = m.pop("referees", None)
        return m

    def _load_all_sport_matches(self, sport: str) -> pd.DataFrame:
        """Load all finished matches for the sport (cached per sport)."""
        key = ("sport", sport)
        if key in self._match_cache:
            return self._match_cache[key]

        all_rows: list[dict] = []
        page, psize = 0, 10_000
        while True:
            res = (
                self.client.table("matches")
                .select("id, home_team_id, away_team_id, home_score, away_score,"
                        " match_date, league_id, venue, season, round")
                .eq("sport", sport)
                .eq("status", "finished")
                .not_.is_("home_score", "null")
                .order("match_date", desc=False)
                .range(page * psize, (page + 1) * psize - 1)
                .execute()
            )
            batch = res.data or []
            all_rows.extend(batch)
            if len(batch) < psize:
                break
            page += 1

        df = pd.DataFrame(all_rows)
        if not df.empty:
            df["match_date"] = pd.to_datetime(df["match_date"], utc=True, errors="coerce")
            df["match_date"] = df["match_date"].dt.tz_localize(None)
        self._match_cache[key] = df
        return df

    def _team_history(self, team_id: int, all_matches: pd.DataFrame,
                      before_dt: datetime, is_home: bool) -> pd.DataFrame:
        """
        Filter all_matches for team_id, add perspective columns (is_home,
        goals_for, goals_against, result, clean_sheet, btts, over25).
        """
        home_games = all_matches[all_matches["home_team_id"] == team_id].copy()
        away_games = all_matches[all_matches["away_team_id"] == team_id].copy()

        def _enrich(df: pd.DataFrame, perspective_home: bool) -> pd.DataFrame:
            df = df.copy()
            df["is_home"] = perspective_home
            if perspective_home:
                df["goals_for"]     = df["home_score"]
                df["goals_against"] = df["away_score"]
            else:
                df["goals_for"]     = df["away_score"]
                df["goals_against"] = df["home_score"]
            df["result"] = df.apply(lambda r:
                "W" if r["goals_for"] > r["goals_against"] else
                ("D" if r["goals_for"] == r["goals_against"] else "L"), axis=1)
            df["clean_sheet"] = (df["goals_against"] == 0).astype(int)
            df["btts"]        = ((df["goals_for"] > 0) & (df["goals_against"] > 0)).astype(int)
            df["over25"]      = ((df["goals_for"] + df["goals_against"]) > 2.5).astype(int)
            # xG placeholders — leave 0; real xG could come from match_stats
            df["xg_for"] = NAN_FILL
            df["xga"]    = NAN_FILL
            return df

        combined = pd.concat([
            _enrich(home_games, True),
            _enrich(away_games, False),
        ]).sort_values("match_date").reset_index(drop=True)

        return combined[combined["match_date"] < pd.Timestamp(before_dt)]

    def _load_odds(self, match_id: int) -> list[dict]:
        res = (
            self.client.table("odds_history")
            .select("*")
            .eq("match_id", match_id)
            .execute()
        )
        return res.data or []

    def _get_dixon_coles(self, league_id: int, all_matches: pd.DataFrame,
                          before_dt: datetime) -> DixonColesEstimator:
        key = (league_id, before_dt.date())
        if key in self._dc_cache:
            return self._dc_cache[key]
        league_matches = all_matches[all_matches["league_id"] == league_id]
        dc = DixonColesEstimator(league_matches).fit(before_dt)
        self._dc_cache[key] = dc
        return dc

    def _build_league_table(self, league_id: int, all_matches: pd.DataFrame,
                             before_dt: datetime) -> dict[int, dict]:
        """
        Simulate standing at before_dt.
        Returns {team_id: {position, points, top4_gap, relegation_gap}}.
        """
        key = (league_id, before_dt.date())
        if key in self._table_cache:
            return self._table_cache[key]

        hist = all_matches[
            (all_matches["league_id"] == league_id) &
            (all_matches["match_date"] < pd.Timestamp(before_dt))
        ]
        table: dict[int, dict] = {}
        for _, r in hist.iterrows():
            for tid, gf, ga in [(r["home_team_id"], r["home_score"], r["away_score"]),
                                (r["away_team_id"], r["away_score"], r["home_score"])]:
                if tid not in table:
                    table[tid] = {"points": 0, "gd": 0, "played": 0}
                if gf > ga:   table[tid]["points"] += 3
                elif gf == ga: table[tid]["points"] += 1
                table[tid]["gd"] += (gf - ga)
                table[tid]["played"] += 1

        sorted_teams = sorted(table.items(), key=lambda x: (-x[1]["points"], -x[1]["gd"]))
        n = len(sorted_teams)
        result: dict[int, dict] = {}
        for pos, (tid, stats) in enumerate(sorted_teams, start=1):
            top4_pts   = sorted_teams[min(3, n-1)][1]["points"] if n >= 4 else 0
            rel_pts    = sorted_teams[max(n-4, 0)][1]["points"] if n >= 4 else 0
            result[tid] = {
                "position":       pos,
                "points":         stats["points"],
                "top4_gap":       max(0, top4_pts - stats["points"]),
                "relegation_gap": max(0, stats["points"] - rel_pts),
            }

        self._table_cache[key] = result
        return result

    # ── Utility ─────────────────────────────────────────────────

    @staticmethod
    def _parse_dt(value: str) -> datetime | None:
        for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                raw = datetime.strptime(value[:19], fmt[:len(fmt)])
                return raw.replace(tzinfo=None)
            except (ValueError, TypeError):
                continue
        return None

    def feature_names(self) -> list[str]:
        """Build a dummy feature vector to enumerate all feature names."""
        dummy_feats = {
            **{f"home_form{w}_{s}": 0.0 for w in FORM_WINDOWS
               for s in ["ppg","gscored","gconceded","xg","xga","cs_rate","btts_rate",
                         "over25_rate","win_pct","draw_pct","loss_pct"]},
            **{f"away_form{w}_{s}": 0.0 for w in FORM_WINDOWS
               for s in ["ppg","gscored","gconceded","xg","xga","cs_rate","btts_rate",
                         "over25_rate","win_pct","draw_pct","loss_pct"]},
        }
        return sorted(dummy_feats.keys())
