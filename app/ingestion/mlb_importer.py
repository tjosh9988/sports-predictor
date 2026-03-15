"""
mlb_importer.py — Imports historical MLB game data.

Handles:
  - Retrosheet/Baseball-Reference CSV format
    (Date, Home, Visitor, HmRuns, VisRuns)
  - Kaggle MLB game logs
    (date, v_name, h_name, v_score, h_score)
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from app.base_importer import BaseImporter

logger = logging.getLogger(__name__)

MLB_ALIASES: dict[str, str] = {
    "nya": "new york yankees",
    "nyy": "new york yankees",
    "nyn": "new york mets",
    "nym": "new york mets",
    "bos": "boston red sox",
    "tor": "toronto blue jays",
    "bal": "baltimore orioles",
    "tba": "tampa bay rays",
    "tb":  "tampa bay rays",
    "cle": "cleveland guardians",
    "cleveland indians": "cleveland guardians",
    "cha": "chicago white sox",
    "chw": "chicago white sox",
    "det": "detroit tigers",
    "kca": "kansas city royals",
    "kc":  "kansas city royals",
    "min": "minnesota twins",
    "hou": "houston astros",
    "laa": "los angeles angels",
    "ana": "los angeles angels",
    "oax": "oakland athletics",
    "oak": "oakland athletics",
    "sea": "seattle mariners",
    "tex": "texas rangers",
    "lan": "los angeles dodgers",
    "lad": "los angeles dodgers",
    "sfn": "san francisco giants",
    "sf":  "san francisco giants",
    "sdn": "san diego padres",
    "sd":  "san diego padres",
    "col": "colorado rockies",
    "ari": "arizona diamondbacks",
    "chn": "chicago cubs",
    "chc": "chicago cubs",
    "sln": "st. louis cardinals",
    "cin": "cincinnati reds",
    "mil": "milwaukee brewers",
    "pit": "pittsburgh pirates",
    "atl": "atlanta braves",
    "mia": "miami marlins",
    "flo": "miami marlins",
    "montreal expos": "washington nationals",
    "mon": "washington nationals",
    "was": "washington nationals",
    "phi": "philadelphia phillies",
    "nle": "new york mets",
}


class MLBImporter(BaseImporter):

    sport_slug = "mlb"

    def load_aliases(self) -> dict[str, str]:
        return MLB_ALIASES

    def normalize(self, df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
        df = self._clean_df(df)
        df.columns = [c.lower().strip() for c in df.columns]
        col = set(df.columns)

        if "v_name" in col or "h_name" in col:
            return self._normalize_retrosheet(df)
        if "hometeam" in col or "visiting_team" in col:
            return self._normalize_bref(df)
        return self._normalize_generic(df)

    def _normalize_retrosheet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Retrosheet / Kaggle GL (game log) format."""
        col = df.columns.tolist()
        date_col  = next((c for c in col if "date" in c), None)
        home_col  = "h_name" if "h_name" in col else next((c for c in col if "home" in c and "team" in c), None)
        away_col  = "v_name" if "v_name" in col else next((c for c in col if "visitor" in c or "away" in c), None)
        hscore    = next((c for c in col if c in ("h_score", "hmruns", "home_score")), None)
        ascore    = next((c for c in col if c in ("v_score", "visruns", "away_score")), None)
        season    = next((c for c in col if "year" in c or "season" in c), None)

        if not home_col or not away_col or not date_col:
            return pd.DataFrame()

        rows = []
        for _, r in df.iterrows():
            date_raw = str(r.get(date_col, ""))
            # Retrosheet date is YYYYMMDD integer
            if date_raw.isdigit() and len(date_raw) == 8:
                date_raw = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
            rows.append({
                "home_team_name": str(r.get(home_col, "")),
                "away_team_name": str(r.get(away_col, "")),
                "match_date":     date_raw,
                "league_name":    "MLB",
                "country":        "USA",
                "season":         str(r.get(season, date_raw[:4])) if season else date_raw[:4],
                "home_score":     r.get(hscore) if hscore else None,
                "away_score":     r.get(ascore) if ascore else None,
                "status":         "finished",
            })
        return pd.DataFrame(rows)

    def _normalize_bref(self, df: pd.DataFrame) -> pd.DataFrame:
        col = df.columns.tolist()
        date_col  = next((c for c in col if "date" in c), None)
        home_col  = next((c for c in col if "hometeam" in c or "home_team" in c), None)
        away_col  = next((c for c in col if "visiting" in c or "away_team" in c), None)
        hscore    = next((c for c in col if c in ("r", "rhome", "home_r", "home_runs")), None)
        ascore    = next((c for c in col if c in ("r.1", "raway", "away_r", "vis_runs")), None)

        if not date_col or not home_col or not away_col:
            return self._normalize_generic(df)

        rows = []
        for _, r in df.iterrows():
            date_raw = str(r.get(date_col, ""))
            if not date_raw or date_raw.lower() in ("nan", "date"):
                continue
            rows.append({
                "home_team_name": str(r.get(home_col, "")),
                "away_team_name": str(r.get(away_col, "")),
                "match_date":     date_raw,
                "league_name":    "MLB",
                "country":        "USA",
                "season":         date_raw[:4] if len(date_raw) >= 4 else "",
                "home_score":     r.get(hscore) if hscore else None,
                "away_score":     r.get(ascore) if ascore else None,
                "status":         "finished",
            })
        return pd.DataFrame(rows)

    def _normalize_generic(self, df: pd.DataFrame) -> pd.DataFrame:
        col = df.columns.tolist()
        home_col = next((c for c in col if "home" in c and "team" in c), None)
        away_col = next((c for c in col if "away" in c and "team" in c), None)
        date_col = next((c for c in col if "date" in c), None)
        if not home_col or not away_col or not date_col:
            return pd.DataFrame()
        rows = []
        for _, r in df.iterrows():
            rows.append({
                "home_team_name": str(r[home_col]),
                "away_team_name": str(r[away_col]),
                "match_date":     str(r[date_col]),
                "league_name":    "MLB",
                "country":        "USA",
                "status":         "finished",
            })
        return pd.DataFrame(rows)
