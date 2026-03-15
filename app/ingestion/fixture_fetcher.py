"""
fixture_fetcher.py — Live fixture pipeline for all sports.

Responsibilities
----------------
1. Fetch upcoming fixtures (next 7 days) from API-Sports for every sport
2. Fetch odds from OddsPapi (primary) and The Odds API (backup)
3. Merge odds from both sources; use Pinnacle lines as sharp-money benchmark
4. Upsert matches + odds into Supabase
5. Run as an APScheduler job at 06:00 daily (and on-demand)

Rate limits handled with exponential-backoff retry via tenacity.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from ..config import settings
from ..database import get_supabase_admin

logger = logging.getLogger(__name__)

# ─────────────────────────── Constants ─────────────────────────────────────

API_SPORTS_BASE  = "https://v3.football.api-sports.io"
ODDSPAPI_BASE    = "https://api.oddspapi.com/v1"
THE_ODDS_BASE    = "https://api.the-odds-api.com/v4"
PINNACLE_NAME    = "Pinnacle"

SPORT_CONFIGS: list[dict] = [
    {"slug": "football",    "api_sports_sport": "football",    "odds_sport": "soccer",
     "api_sports_leagues": [39, 140, 135, 78, 61, 2, 3, 848]},  # EPL, La Liga, etc.
    {"slug": "basketball",  "api_sports_sport": "basketball",  "odds_sport": "basketball_nba",
     "api_sports_leagues": [12]},  # NBA
    {"slug": "tennis",      "api_sports_sport": "tennis",      "odds_sport": "tennis",
     "api_sports_leagues": [1, 2]},
    {"slug": "nfl",         "api_sports_sport": "american-football", "odds_sport": "americanfootball_nfl",
     "api_sports_leagues": [1]},
    {"slug": "cricket",     "api_sports_sport": "cricket",     "odds_sport": "cricket",
     "api_sports_leagues": [1, 2, 6]},  # IPL, T20I, ODI
    {"slug": "nhl",         "api_sports_sport": "hockey",      "odds_sport": "icehockey_nhl",
     "api_sports_leagues": [57]},
    {"slug": "mlb",         "api_sports_sport": "baseball",    "odds_sport": "baseball_mlb",
     "api_sports_leagues": [1]},
]

MARKETS_MAP = {
    "h2h":    "1X2",
    "spreads":"Handicap",
    "totals": "Over/Under",
    "btts":   "BTTS",
}

# ─────────────────────────── HTTP Client ───────────────────────────────────

def _make_client(headers: dict) -> httpx.Client:
    return httpx.Client(timeout=15.0, headers=headers)


# ─────────────────────────── Retry Decorator ───────────────────────────────

def _retry(fn):
    """Decorator: retry on httpx errors / 429 with exponential backoff."""
    return retry(
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )(fn)


# ─────────────────────────── API-Sports Client ─────────────────────────────

class APISportsClient:
    def __init__(self):
        self._client = _make_client({
            "x-rapidapi-host": "v3.football.api-sports.io",
            "x-rapidapi-key":  settings.API_SPORTS_KEY,
        })

    @_retry
    def get_fixtures(self, sport: str, league_id: int, from_date: str, to_date: str) -> list[dict]:
        base = f"https://v3.{sport}.api-sports.io"
        resp = self._client.get(f"{base}/fixtures", params={
            "league": league_id,
            "from":   from_date,
            "to":     to_date,
            "status": "NS",   # Not Started
            "timezone": "UTC",
        })
        resp.raise_for_status()
        data = resp.json()
        if data.get("errors"):
            logger.warning("API-Sports error: %s", data["errors"])
            return []
        return data.get("response", [])

    @_retry
    def get_result(self, sport: str, fixture_id: int) -> dict | None:
        base = f"https://v3.{sport}.api-sports.io"
        resp = self._client.get(f"{base}/fixtures", params={"id": fixture_id})
        resp.raise_for_status()
        items = resp.json().get("response", [])
        return items[0] if items else None

    def close(self):
        self._client.close()


# ─────────────────────────── OddsPapi Client ───────────────────────────────

class OddspapiClient:
    def __init__(self):
        self._client = _make_client({"Authorization": f"Bearer {settings.ODDS_API_KEY}"})

    @_retry
    def get_odds(self, sport: str, markets: list[str]) -> list[dict]:
        if not settings.ODDS_API_KEY:
            return []
        resp = self._client.get(f"{ODDSPAPI_BASE}/odds", params={
            "sport":   sport,
            "markets": ",".join(markets),
        })
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "10"))
            logger.warning("OddsPapi 429 — sleeping %ds", retry_after)
            time.sleep(retry_after)
            resp.raise_for_status()
        resp.raise_for_status()
        return resp.json().get("data", [])

    def close(self):
        self._client.close()


# ─────────────────────────── The Odds API Client ───────────────────────────

class TheOddsAPIClient:
    def __init__(self):
        self._client = _make_client({})

    @_retry
    def get_odds(self, sport_key: str, markets: str = "h2h,totals") -> list[dict]:
        resp = self._client.get(f"{THE_ODDS_BASE}/sports/{sport_key}/odds", params={
            "apiKey":  settings.THE_ODDS_API_KEY,
            "regions": "eu,uk",
            "markets": markets,
            "oddsFormat": "decimal",
            "dateFormat": "iso",
        })
        if resp.status_code == 422:
            logger.warning("The Odds API: unsupported sport '%s'", sport_key)
            return []
        resp.raise_for_status()
        remaining = resp.headers.get("x-requests-remaining")
        logger.debug("The Odds API: %s requests remaining", remaining)
        return resp.json()

    def close(self):
        self._client.close()


# ─────────────────────────── Odds Merger ───────────────────────────────────

class OddsMerger:
    """
    Combines odds from OddsPapi (primary) and The Odds API (backup).
    Prefer Pinnacle lines as the sharp-money benchmark when available.
    The closing line from Pinnacle is stored as opening_* for comparison.
    """

    @staticmethod
    def merge(
        oddspapi_events: list[dict],
        theodds_events: list[dict],
        home_team: str,
        away_team: str,
    ) -> list[dict]:
        """
        Returns a list of normalised odds records (one per bookmaker+market).
        """
        records: list[dict] = []

        # ── OddsPapi ─────────────────────────────────────────────
        for event in oddspapi_events:
            if not OddsMerger._match_teams(event, home_team, away_team):
                continue
            for bk in event.get("bookmakers", []):
                bk_name = bk.get("key", bk.get("title", "unknown"))
                for mkt in bk.get("markets", []):
                    market_key = MARKETS_MAP.get(mkt.get("key", ""), mkt.get("key", ""))
                    outcomes = {o["name"].lower(): o["price"] for o in mkt.get("outcomes", [])}
                    records.append({
                        "bookmaker":     bk_name,
                        "market":        market_key,
                        "opening_home":  outcomes.get("home") or outcomes.get(home_team.lower()),
                        "opening_draw":  outcomes.get("draw"),
                        "opening_away":  outcomes.get("away") or outcomes.get(away_team.lower()),
                        "closing_home":  None,
                        "closing_draw":  None,
                        "closing_away":  None,
                        "_source":       "oddspapi",
                    })

        # ── The Odds API (backup / additional bookmakers) ─────────
        for event in theodds_events:
            if not OddsMerger._match_teams(event, home_team, away_team):
                continue
            for bk in event.get("bookmakers", []):
                bk_name = bk.get("key", "")
                for mkt in bk.get("markets", []):
                    market_key = MARKETS_MAP.get(mkt.get("key", ""), mkt.get("key", ""))
                    # Skip if we already have this bookmaker+market from OddsPapi
                    already_have = any(
                        r["bookmaker"] == bk_name and r["market"] == market_key
                        for r in records
                    )
                    if already_have:
                        # Promote to closing line if it's Pinnacle
                        if PINNACLE_NAME.lower() in bk_name.lower():
                            for r in records:
                                if r["bookmaker"] == bk_name and r["market"] == market_key:
                                    outcomes = {o["name"].lower(): o["price"] for o in mkt.get("outcomes", [])}
                                    r["closing_home"] = outcomes.get("home") or outcomes.get(home_team.lower())
                                    r["closing_draw"] = outcomes.get("draw")
                                    r["closing_away"] = outcomes.get("away") or outcomes.get(away_team.lower())
                        continue

                    outcomes = {o["name"].lower(): o["price"] for o in mkt.get("outcomes", [])}
                    records.append({
                        "bookmaker":    bk_name,
                        "market":       market_key,
                        "opening_home": outcomes.get("home") or outcomes.get(home_team.lower()),
                        "opening_draw": outcomes.get("draw"),
                        "opening_away": outcomes.get("away") or outcomes.get(away_team.lower()),
                        "closing_home": None,
                        "closing_draw": None,
                        "closing_away": None,
                        "_source":      "theodds",
                    })

        # Clean up internal key
        for r in records:
            r.pop("_source", None)

        return records

    @staticmethod
    def _match_teams(event: dict, home: str, away: str) -> bool:
        ht = str(event.get("home_team", "")).lower()
        at = str(event.get("away_team", "")).lower()
        return (
            home.lower()[:6] in ht or ht[:6] in home.lower()
        ) and (
            away.lower()[:6] in at or at[:6] in away.lower()
        )


# ─────────────────────────── Main Fixture Fetcher ──────────────────────────

class FixtureFetcher:
    def __init__(self):
        self.supabase  = get_supabase_admin()
        self.api_sports = APISportsClient()
        self.oddspapi   = OddspapiClient()
        self.theodds    = TheOddsAPIClient()
        self._sport_cache: dict[str, int] = {}

    def run(self) -> None:
        logger.info("🏟️  FixtureFetcher starting — %s UTC", datetime.now(timezone.utc).isoformat())
        from_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        to_date   = (datetime.now(timezone.utc) + timedelta(days=7)).strftime("%Y-%m-%d")

        total_fixtures = total_odds = 0
        for cfg in SPORT_CONFIGS:
            f, o = self._process_sport(cfg, from_date, to_date)
            total_fixtures += f
            total_odds     += o

        logger.info("✅ Done — %d fixtures, %d odds rows upserted", total_fixtures, total_odds)

    def _process_sport(self, cfg: dict, from_date: str, to_date: str) -> tuple[int, int]:
        slug       = cfg["slug"]
        sport_name = cfg["api_sports_sport"]
        odds_sport = cfg["odds_sport"]
        leagues    = cfg["api_sports_leagues"]
        logger.info("[%s] Fetching fixtures …", slug)

        # Fetch from multiple sources
        all_fixtures: list[dict] = []
        for lid in leagues:
            try:
                fixtures = self.api_sports.get_fixtures(sport_name, lid, from_date, to_date)
                all_fixtures.extend(fixtures)
            except Exception as exc:
                logger.error("[%s] API-Sports league=%d failed: %s", slug, lid, exc)

        # Fetch odds once for the sport (both sources)
        try:
            oddspapi_events = self.oddspapi.get_odds(odds_sport, ["h2h", "totals"])
        except Exception as exc:
            logger.warning("[%s] OddsPapi failed: %s", slug, exc)
            oddspapi_events = []

        try:
            theodds_events = self.theodds.get_odds(odds_sport)
        except Exception as exc:
            logger.warning("[%s] TheOddsAPI failed: %s", slug, exc)
            theodds_events = []

        fixtures_upserted = odds_upserted = 0
        sport_id = self._get_sport_id(slug)
        if not sport_id:
            logger.error("[%s] sport not found in DB — skipping", slug)
            return 0, 0

        for fixture in all_fixtures:
            try:
                match_id = self._upsert_fixture(fixture, sport_id)
                if match_id:
                    fixtures_upserted += 1
                    home = self._team_name(fixture, "home")
                    away = self._team_name(fixture, "away")
                    merged = OddsMerger.merge(oddspapi_events, theodds_events, home, away)
                    if merged:
                        odds_rows = [{"match_id": match_id, "recorded_at": datetime.utcnow().isoformat(), **r} for r in merged]
                        self.supabase.table("odds_history").upsert(
                            odds_rows, on_conflict="match_id,bookmaker,market"
                        ).execute()
                        odds_upserted += len(odds_rows)
            except Exception as exc:
                logger.error("[%s] Error processing fixture: %s", slug, exc, exc_info=True)

        logger.info("[%s] Upserted %d fixtures, %d odds rows", slug, fixtures_upserted, odds_upserted)
        return fixtures_upserted, odds_upserted

    def _upsert_fixture(self, fixture: dict, sport_id: int) -> int | None:
        """Upsert a single fixture. Returns the match DB id."""
        teams    = fixture.get("teams", {})
        home_t   = teams.get("home", {})
        away_t   = teams.get("away", {})
        fx_info  = fixture.get("fixture", {})
        league   = fixture.get("league", {})

        home_id  = self._get_or_create_team(sport_id, home_t.get("name", ""), home_t.get("id"))
        away_id  = self._get_or_create_team(sport_id, away_t.get("name", ""), away_t.get("id"))
        league_id = self._get_or_create_league(sport_id, league.get("name", "Unknown"),
                                                league.get("country", ""))

        match_date = fx_info.get("date", "")
        if not match_date or not home_id or not away_id:
            return None

        row = {
            "sport_id":     sport_id,
            "league_id":    league_id,
            "home_team_id": home_id,
            "away_team_id": away_id,
            "match_date":   match_date,
            "status":       "upcoming",
            "venue":        fx_info.get("venue", {}).get("name"),
            "season":       str(league.get("season", "")),
            "round":        league.get("round"),
        }
        res = self.supabase.table("matches").upsert(
            row, on_conflict="home_team_id,away_team_id,match_date"
        ).execute()
        return res.data[0]["id"] if res.data else None

    # ── Helpers ────────────────────────────────────────────────

    @staticmethod
    def _team_name(fixture: dict, side: str) -> str:
        return fixture.get("teams", {}).get(side, {}).get("name", "")

    def _get_sport_id(self, slug: str) -> int | None:
        if slug in self._sport_cache:
            return self._sport_cache[slug]
        res = self.supabase.table("sports").select("id").eq("slug", slug).single().execute()
        sid = res.data["id"] if res.data else None
        if sid:
            self._sport_cache[slug] = sid
        return sid

    def _get_or_create_team(self, sport_id: int, name: str, ext_id: Any = None) -> int | None:
        if not name:
            return None
        res = self.supabase.table("teams").upsert(
            {"sport_id": sport_id, "name": name, "elo_rating": 1500},
            on_conflict="sport_id,name",
        ).execute()
        return res.data[0]["id"] if res.data else None

    def _get_or_create_league(self, sport_id: int, name: str, country: str) -> int | None:
        res = self.supabase.table("leagues").upsert(
            {"sport_id": sport_id, "name": name, "country": country},
            on_conflict="sport_id,name,country",
        ).execute()
        return res.data[0]["id"] if res.data else None

    def close(self):
        self.api_sports.close()
        self.oddspapi.close()
        self.theodds.close()


# ─────────────────────────── APScheduler Entry Point ───────────────────────

def run_scheduler() -> None:
    """
    Starts a blocking APScheduler that fires FixtureFetcher.run()
    every day at 06:00 UTC. Also fires immediately on startup.
    """
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_job(
        _scheduled_job,
        trigger=CronTrigger(hour=6, minute=0),
        id="fixture_fetcher_daily",
        name="Daily fixture + odds fetch",
        max_instances=1,
        misfire_grace_time=3600,   # tolerate up to 1h late start
        replace_existing=True,
    )
    logger.info("📅 Scheduler started — FixtureFetcher fires daily at 06:00 UTC")
    # Run immediately on first start
    _scheduled_job()
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


def _scheduled_job() -> None:
    fetcher = FixtureFetcher()
    try:
        fetcher.run()
    finally:
        fetcher.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_scheduler()
