"""
result_fetcher.py — Resolves PENDING predictions against actual match results.

Responsibilities
----------------
1. Poll Supabase every 2 hours for all PENDING predictions
   whose match_date is in the past
2. Fetch actual results from API-Sports
3. Update predictions.status → CORRECT | INCORRECT
4. Update accumulator_legs.status and compute accumulator.status
5. Write actual match stats to match_stats and prediction_results
6. When 200+ new resolutions accumulate, set a retraining flag in Redis

APScheduler interval: every 2 hours
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import httpx

from ..config import settings
from ..database import get_supabase_admin
from ..redis_client import get_redis, cache_set, cache_get

logger = logging.getLogger(__name__)

RETRAIN_FLAG_KEY     = "ml:retrain_needed"
RETRAIN_COUNTER_KEY  = "ml:new_results_since_last_train"
RETRAIN_THRESHOLD    = 200

API_SPORTS_SPORT_MAP: dict[str, str] = {
    "football":   "football",
    "basketball": "basketball",
    "tennis":     "tennis",
    "nfl":        "american-football",
    "cricket":    "cricket",
    "nhl":        "hockey",
    "mlb":        "baseball",
}


# ─────────────────────────── Retry Decorator ───────────────────────────────

def _api_retry(fn):
    return retry(
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(fn)


# ─────────────────────────── API-Sports Result Client ──────────────────────

class ResultAPIClient:

    def __init__(self):
        self._http = httpx.Client(
            timeout=15.0,
            headers={
                "x-rapidapi-host": "v3.football.api-sports.io",
                "x-rapidapi-key":  settings.API_SPORTS_KEY,
            },
        )

    @_api_retry
    def get_fixture(self, sport_slug: str, external_fixture_id: int) -> dict | None:
        sport = API_SPORTS_SPORT_MAP.get(sport_slug, "football")
        base  = f"https://v3.{sport}.api-sports.io"
        resp  = self._http.get(f"{base}/fixtures", params={"id": external_fixture_id})
        resp.raise_for_status()
        items = resp.json().get("response", [])
        return items[0] if items else None

    @_api_retry
    def search_fixture(self, sport_slug: str, home: str, away: str, date: str) -> dict | None:
        """Fallback search by team names + date when external_id is unknown."""
        sport = API_SPORTS_SPORT_MAP.get(sport_slug, "football")
        base  = f"https://v3.{sport}.api-sports.io"
        resp  = self._http.get(f"{base}/fixtures", params={
            "team_name": home,
            "date":      date[:10],
            "timezone":  "UTC",
        })
        resp.raise_for_status()
        items = resp.json().get("response", [])
        for item in items:
            teams = item.get("teams", {})
            if (away.lower()[:5] in teams.get("away", {}).get("name", "").lower() or
                    home.lower()[:5] in teams.get("home", {}).get("name", "").lower()):
                return item
        return None

    def close(self):
        self._http.close()


# ─────────────────────────── Outcome Resolver ──────────────────────────────

class OutcomeResolver:
    """
    Determines whether a prediction was CORRECT or INCORRECT given match result.
    """

    @staticmethod
    def resolve(market: str, predicted_outcome: str, home_score: int, away_score: int) -> str:
        """Returns 'CORRECT' or 'INCORRECT'."""
        market = market.upper()
        predicted = predicted_outcome.strip().upper()

        try:
            if "1X2" in market or "H2H" in market:
                if home_score > away_score:
                    actual = "HOME"
                elif away_score > home_score:
                    actual = "AWAY"
                else:
                    actual = "DRAW"
                return "CORRECT" if predicted == actual else "INCORRECT"

            if "OVER/UNDER" in market or "TOTALS" in market:
                # e.g. "Over 2.5"
                parts = predicted.split()
                direction = parts[0] if parts else ""
                line = float(parts[1]) if len(parts) > 1 else 2.5
                total = home_score + away_score
                if direction == "OVER":
                    return "CORRECT" if total > line else "INCORRECT"
                else:
                    return "CORRECT" if total < line else "INCORRECT"

            if "BTTS" in market:
                both = home_score > 0 and away_score > 0
                return "CORRECT" if (predicted == "YES") == both else "INCORRECT"

            if "HANDICAP" in market or "SPREAD" in market:
                # e.g. "Home -1.5"
                parts = predicted.split()
                side = parts[0] if parts else "HOME"
                handicap = float(parts[1]) if len(parts) > 1 else 0.0
                margin = (home_score - away_score) if side.upper() == "HOME" else (away_score - home_score)
                return "CORRECT" if (margin + handicap) > 0 else "INCORRECT"

        except Exception as exc:
            logger.warning("Outcome resolve error (market=%s, pred=%s): %s", market, predicted_outcome, exc)

        return "INCORRECT"


# ─────────────────────────── Result Fetcher ────────────────────────────────

class ResultFetcher:

    def __init__(self):
        self.supabase = get_supabase_admin()
        self.api      = ResultAPIClient()
        self._new_resolutions = 0

    def run(self) -> None:
        logger.info("🔍 ResultFetcher starting — %s UTC", datetime.now(timezone.utc).isoformat())
        pending = self._fetch_pending_predictions()
        logger.info("Found %d PENDING predictions to check", len(pending))

        for pred in pending:
            try:
                self._resolve_prediction(pred)
            except Exception as exc:
                logger.error("Error resolving prediction %d: %s", pred["id"], exc, exc_info=True)

        if self._new_resolutions > 0:
            self._update_retrain_counter(self._new_resolutions)

        logger.info("✅ ResultFetcher done — resolved %d predictions", self._new_resolutions)
        self._new_resolutions = 0

    # ── Pending predictions ─────────────────────────────────────

    def _fetch_pending_predictions(self) -> list[dict]:
        """
        Fetch PENDING predictions whose match finished in the past.
        JOIN through matches to get scores and team names.
        """
        now = datetime.now(timezone.utc).isoformat()
        res = (
            self.supabase.table("predictions")
            .select(
                "id, match_id, market, predicted_outcome, "
                "matches(id, sport_id, match_date, status, home_score, away_score, "
                "home_team_id, away_team_id, "
                "sports(slug), "
                "home:teams!home_team_id(name), away:teams!away_team_id(name))"
            )
            .eq("status", "PENDING")
            .lt("matches.match_date", now)
            .execute()
        )
        return res.data or []

    # ── Core resolution ─────────────────────────────────────────

    def _resolve_prediction(self, pred: dict) -> None:
        match_data = pred.get("matches", {})
        if not match_data:
            return

        home_score = match_data.get("home_score")
        away_score = match_data.get("away_score")

        # If scores not in DB, fetch from API-Sports
        if home_score is None or away_score is None:
            sport_slug = (match_data.get("sports") or {}).get("slug", "football")
            home_name  = (match_data.get("home") or {}).get("name", "")
            away_name  = (match_data.get("away") or {}).get("name", "")
            match_date = match_data.get("match_date", "")
            api_result = self.api.search_fixture(sport_slug, home_name, away_name, match_date)
            if not api_result:
                logger.debug("No API result yet for prediction %d", pred["id"])
                return
            home_score, away_score, stats = self._extract_scores_and_stats(api_result)
            if home_score is None:
                return
            # Update match scores in DB
            self.supabase.table("matches").update({
                "home_score": home_score,
                "away_score": away_score,
                "status":     "finished",
            }).eq("id", match_data["id"]).execute()
            # Store match stats
            self._upsert_match_stats(match_data["id"], stats)
        else:
            stats = {}

        # Resolve outcome
        status = OutcomeResolver.resolve(
            pred["market"],
            pred["predicted_outcome"],
            home_score,
            away_score,
        )

        # Determine actual outcome string
        if home_score > away_score:
            actual_outcome = "Home"
        elif away_score > home_score:
            actual_outcome = "Away"
        else:
            actual_outcome = "Draw"

        # Update prediction
        self.supabase.table("predictions").update({
            "status":         status,
            "actual_outcome": actual_outcome,
        }).eq("id", pred["id"]).execute()

        # Write prediction_results row
        self.supabase.table("prediction_results").upsert({
            "prediction_id": pred["id"],
            "actual_result": actual_outcome,
            "goals_home":    home_score,
            "goals_away":    away_score,
            "resolved_at":   datetime.utcnow().isoformat(),
        }, on_conflict="prediction_id").execute()

        # Update accumulator legs
        self._update_accumulator_legs(pred["id"], status)

        self._new_resolutions += 1
        logger.debug("Prediction %d → %s", pred["id"], status)

    # ── Match stats ─────────────────────────────────────────────

    def _extract_scores_and_stats(self, api_result: dict) -> tuple[int | None, int | None, dict]:
        goals = api_result.get("goals", {})
        home_score = goals.get("home")
        away_score = goals.get("away")

        stats: dict[str, tuple] = {}
        for team_stat in api_result.get("statistics", []):
            side = "home" if team_stat.get("team", {}).get("id") == api_result.get("teams", {}).get("home", {}).get("id") else "away"
            for stat in team_stat.get("statistics", []):
                stat_type = stat.get("type", "")
                val = stat.get("value")
                if stat_type not in stats:
                    stats[stat_type] = [None, None]
                idx = 0 if side == "home" else 1
                stats[stat_type][idx] = self._safe_int(val)

        return home_score, away_score, stats

    def _upsert_match_stats(self, match_id: int, stats: dict) -> None:
        if not stats:
            return
        rows = [
            {
                "match_id":   match_id,
                "stat_type":  stat_type,
                "home_value": vals[0],
                "away_value": vals[1],
            }
            for stat_type, vals in stats.items()
        ]
        if rows:
            self.supabase.table("match_stats").upsert(
                rows, on_conflict="match_id,stat_type"
            ).execute()

    # ── Accumulator resolution ──────────────────────────────────

    def _update_accumulator_legs(self, prediction_id: int, leg_status: str) -> None:
        """Update the leg, then recompute the parent accumulator status."""
        # Update the leg
        self.supabase.table("accumulator_legs").update(
            {"status": leg_status}
        ).eq("prediction_id", prediction_id).execute()

        # Find all accumulators containing this prediction
        legs_res = (
            self.supabase.table("accumulator_legs")
            .select("accumulator_id")
            .eq("prediction_id", prediction_id)
            .execute()
        )
        acca_ids = {row["accumulator_id"] for row in (legs_res.data or [])}

        for acca_id in acca_ids:
            self._recompute_accumulator_status(acca_id)

    def _recompute_accumulator_status(self, accumulator_id: int) -> None:
        """
        Recompute accumulator status:
        - If any leg is INCORRECT → LOST
        - If all legs are CORRECT → WON
        - Otherwise → still PENDING
        """
        legs_res = (
            self.supabase.table("accumulator_legs")
            .select("status")
            .eq("accumulator_id", accumulator_id)
            .execute()
        )
        legs = legs_res.data or []
        if not legs:
            return

        statuses = [l["status"] for l in legs]
        if "INCORRECT" in statuses:
            new_status = "LOST"
        elif all(s == "CORRECT" for s in statuses):
            new_status = "WON"
        else:
            new_status = "PENDING"

        self.supabase.table("accumulators").update(
            {"status": new_status}
        ).eq("id", accumulator_id).execute()
        logger.debug("Accumulator %d → %s", accumulator_id, new_status)

    # ── Retraining flag ─────────────────────────────────────────

    def _update_retrain_counter(self, count: int) -> None:
        """
        Increments a Redis counter. When it crosses RETRAIN_THRESHOLD,
        sets a flag that the ML service polls to trigger retraining.
        """
        try:
            redis = get_redis()
            new_count = redis.incrby(RETRAIN_COUNTER_KEY, count)
            logger.info("Retrain counter: %d / %d", new_count, RETRAIN_THRESHOLD)
            if new_count >= RETRAIN_THRESHOLD:
                redis.set(RETRAIN_FLAG_KEY, "1")
                redis.set(RETRAIN_COUNTER_KEY, "0")   # reset
                logger.info(
                    "🚀 Retraining flag SET — %d new results accumulated", new_count
                )
        except Exception as exc:
            logger.error("Failed to update retrain counter: %s", exc)

    @staticmethod
    def _safe_int(val: Any) -> int | None:
        try:
            return int(float(str(val).replace("%", "")))
        except (TypeError, ValueError):
            return None

    def close(self):
        self.api.close()


# ─────────────────────────── APScheduler Entry Point ───────────────────────

def run_result_scheduler() -> None:
    """
    Starts a BackgroundScheduler that fires ResultFetcher.run() every 2 hours.
    Returns the scheduler so callers can integrate it with other schedulers.
    """
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        _result_job,
        trigger=IntervalTrigger(hours=2),
        id="result_fetcher",
        name="Resolve PENDING predictions every 2h",
        max_instances=1,
        misfire_grace_time=600,
        replace_existing=True,
    )
    scheduler.start()
    logger.info("📅 ResultFetcher scheduler started — interval: 2h")
    # Trigger immediately on first call
    _result_job()
    return scheduler


def _result_job() -> None:
    fetcher = ResultFetcher()
    try:
        fetcher.run()
    finally:
        fetcher.close()


if __name__ == "__main__":
    import time
    logging.basicConfig(level=logging.INFO)
    run_result_scheduler()
    # Keep process alive
    try:
        while True:
            time.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        pass
