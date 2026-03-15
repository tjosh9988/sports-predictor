"""
nba_importer.py — Imports historical NBA game data.

Handles:
  - basketball-reference.com CSV export format
    (Date, Home/Neutral, Visitor/Neutral, PTS home, PTS away)
  - Kaggle NBA games dataset
    (GAME_DATE_EST, HOME_TEAM_ID, VISITOR_TEAM_ID, PTS_home, PTS_away)
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from .base_importer import BaseImporter

logger = logging.getLogger(__name__)

NBA_ALIASES: dict[str, str] = {
    "la lakers":          "los angeles lakers",
    "la clippers":        "los angeles clippers",
    "golden state":       "golden state warriors",
    "gsw":                "golden state warriors",
    "okc":                "oklahoma city thunder",
    "new orleans":        "new orleans pelicans",
    "nola":               "new orleans pelicans",
    "ny knicks":          "new york knicks",
    "new york":           "new york knicks",
    "nyk":                "new york knicks",
    "san antonio":        "san antonio spurs",
    "sas":                "san antonio spurs",
    "memphis":            "memphis grizzlies",
    "minnesota":          "minnesota timberwolves",
    "portland":           "portland trail blazers",
    "utah":               "utah jazz",
    "charlotte":          "charlotte hornets",
    "indiana":            "indiana pacers",
    "detroit":            "detroit pistons",
    "chicago":            "chicago bulls",
    "cleveland":          "cleveland cavaliers",
    "cavs":               "cleveland cavaliers",
    "toronto":            "toronto raptors",
    "boston":             "boston celtics",
    "philadelphia":       "philadelphia 76ers",
    "phila":              "philadelphia 76ers",
    "sixers":             "philadelphia 76ers",
    "washington":         "washington wizards",
    "orlando":            "orlando magic",
    "miami":              "miami heat",
    "atlanta":            "atlanta hawks",
    "brooklyn":           "brooklyn nets",
    "denver":             "denver nuggets",
    "dallas":             "dallas mavericks",
    "mavs":               "dallas mavericks",
    "sacramento":         "sacramento kings",
    "phoenix":            "phoenix suns",
}

NBA_TEAM_IDS: dict[int, str] = {
    # Kaggle team ID → readable name (expand from actual dataset)
    1610612737: "atlanta hawks", 1610612738: "boston celtics",
    1610612739: "cleveland cavaliers", 1610612740: "new orleans pelicans",
    1610612741: "chicago bulls", 1610612742: "dallas mavericks",
    1610612743: "denver nuggets", 1610612744: "golden state warriors",
    1610612745: "houston rockets", 1610612746: "los angeles clippers",
    1610612747: "los angeles lakers", 1610612748: "miami heat",
    1610612749: "milwaukee bucks", 1610612750: "minnesota timberwolves",
    1610612751: "brooklyn nets", 1610612752: "new york knicks",
    1610612753: "orlando magic", 1610612754: "indiana pacers",
    1610612755: "philadelphia 76ers", 1610612756: "phoenix suns",
    1610612757: "portland trail blazers", 1610612758: "sacramento kings",
    1610612759: "san antonio spurs", 1610612760: "oklahoma city thunder",
    1610612761: "toronto raptors", 1610612762: "utah jazz",
    1610612763: "memphis grizzlies", 1610612764: "washington wizards",
    1610612765: "detroit pistons", 1610612766: "charlotte hornets",
}


class NBAImporter(BaseImporter):

    sport_slug = "basketball"

    def load_aliases(self) -> dict[str, str]:
        return NBA_ALIASES

    def normalize(self, df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
        df = self._clean_df(df)
        df.columns = [c.lower().strip() for c in df.columns]
        col = set(df.columns)

        if "home_team_id" in col or "visitor_team_id" in col:
            return self._normalize_kaggle(df)
        return self._normalize_bref(df)

    def _normalize_kaggle(self, df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, r in df.iterrows():
            home_id = self._safe_int(r.get("home_team_id"))
            vis_id  = self._safe_int(r.get("visitor_team_id") or r.get("away_team_id"))
            rows.append({
                "home_team_name": NBA_TEAM_IDS.get(home_id, f"team_{home_id}"),
                "away_team_name": NBA_TEAM_IDS.get(vis_id,  f"team_{vis_id}"),
                "match_date":     str(r.get("game_date_est") or r.get("game_date", "")),
                "league_name":    "NBA",
                "country":        "USA",
                "season":         str(r.get("season", "")),
                "home_score":     r.get("pts_home") or r.get("home_pts"),
                "away_score":     r.get("pts_away") or r.get("visitor_pts"),
                "status":         "finished",
            })
        return pd.DataFrame(rows)

    def _normalize_bref(self, df: pd.DataFrame) -> pd.DataFrame:
        """Basketball-Reference.com export."""
        col = df.columns.tolist()
        date_col  = next((c for c in col if "date" in c), None)
        home_col  = next((c for c in col if "home" in c or c == "home/neutral"), None)
        away_col  = next((c for c in col if "visitor" in c or "away" in c), None)
        hpts_col  = next((c for c in col if c in ("pts", "home_pts", "pts.1", "ptsw")), None)
        apts_col  = next((c for c in col if c in ("pts_away", "pts.2", "ptsl", "visitor_pts")), None)

        if not date_col or not home_col or not away_col:
            logger.warning("NBA: Cannot detect columns in %s", col)
            return pd.DataFrame()

        rows = []
        for _, r in df.iterrows():
            date_raw = str(r.get(date_col, ""))
            if not date_raw or date_raw.lower() in ("nan", "date"):
                continue
            rows.append({
                "home_team_name": str(r.get(home_col, "")),
                "away_team_name": str(r.get(away_col, "")),
                "match_date":     date_raw,
                "league_name":    "NBA",
                "country":        "USA",
                "season":         date_raw[:4] if len(date_raw) >= 4 else "",
                "home_score":     r.get(hpts_col) if hpts_col else None,
                "away_score":     r.get(apts_col) if apts_col else None,
                "status":         "finished",
            })
        return pd.DataFrame(rows)
