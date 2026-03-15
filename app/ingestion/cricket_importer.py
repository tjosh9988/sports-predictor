"""
cricket_importer.py — Imports historical cricket data.

Handles:
  - Cricsheet JSON format (ball-by-ball data with match metadata)
  - Kaggle cricket datasets (matches CSV with team and result fields)
  - Covers: Test, ODI, T20I, IPL, BBL, PSL and other T20 leagues
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from app.base_importer import BaseImporter

logger = logging.getLogger(__name__)

# Map match_type to league
CRICKET_FORMAT_LEAGUES: dict[str, tuple[str, str]] = {
    "test":   ("ICC Test Championship",  "International"),
    "odi":    ("ICC ODI Series",          "International"),
    "t20i":   ("ICC T20I Series",         "International"),
    "ipl":    ("Indian Premier League",   "India"),
    "bbl":    ("Big Bash League",         "Australia"),
    "psl":    ("Pakistan Super League",   "Pakistan"),
    "cpl":    ("Caribbean Premier League","West Indies"),
    "sa20":   ("SA20",                    "South Africa"),
    "the100": ("The Hundred",             "England"),
}

CRICKET_ALIASES: dict[str, str] = {
    "india":              "india",
    "ind":                "india",
    "australia":          "australia",
    "aus":                "australia",
    "england":            "england",
    "eng":                "england",
    "new zealand":        "new zealand",
    "nz":                 "new zealand",
    "south africa":       "south africa",
    "sa":                 "south africa",
    "pakistan":           "pakistan",
    "pak":                "pakistan",
    "sri lanka":          "sri lanka",
    "sl":                 "sri lanka",
    "west indies":        "west indies",
    "wi":                 "west indies",
    "bangladesh":         "bangladesh",
    "ban":                "bangladesh",
    "mumbai indians":     "mumbai indians",
    "mi":                 "mumbai indians",
    "chennai super kings":"chennai super kings",
    "csk":                "chennai super kings",
    "rcb":                "royal challengers bengaluru",
    "royal challengers bangalore": "royal challengers bengaluru",
    "kolkata knight riders": "kolkata knight riders",
    "kkr":                "kolkata knight riders",
    "delhi capitals":     "delhi capitals",
    "dc":                 "delhi capitals",
    "rajasthan royals":   "rajasthan royals",
    "rr":                 "rajasthan royals",
    "sunrisers hyderabad":"sunrisers hyderabad",
    "srh":                "sunrisers hyderabad",
    "punjab kings":       "punjab kings",
    "kxip":               "punjab kings",
    "lucknow super giants":"lucknow super giants",
    "gujarat titans":     "gujarat titans",
}


class CricketImporter(BaseImporter):

    sport_slug = "cricket"

    async def download_from_storage(self):
        """Downloads all files for cricket from Supabase Storage"""
        import os
        folder = "cricket"
        files = self.client.storage.from_("sports-data").list(folder)
        
        local_dir = f"/tmp/stats/{folder}"
        os.makedirs(local_dir, exist_ok=True)
        
        for file in files:
            file_path = f"{folder}/{file['name']}"
            data = self.client.storage.from_("sports-data").download(file_path)
            
            local_file = f"{local_dir}/{file['name']}"
            with open(local_file, "wb") as f:
                f.write(data)
            logger.info("Downloaded %s", file_path)
        
        self.data_dir = Path(local_dir)
        return local_dir

    def load_aliases(self) -> dict[str, str]:
        return CRICKET_ALIASES

    def normalize(self, df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
        df = self._clean_df(df)

        # Cricsheet JSON: load_file returns a DataFrame of metadata already
        if "info" in df.columns or "innings" in df.columns:
            return self._normalize_cricsheet(df, source_file)

        df.columns = [c.lower().strip() for c in df.columns]
        col = set(df.columns)

        if "team1" in col and "team2" in col:
            return self._normalize_kaggle(df)
        return self._normalize_generic(df)

    def _normalize_cricsheet(self, df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
        """Cricsheet stores one match per JSON file as a nested dict."""
        import json
        rows = []
        try:
            with source_file.open(encoding="utf-8") as f:
                data = json.load(f)
            info = data.get("info", {})
            teams = info.get("teams", [])
            if len(teams) < 2:
                return pd.DataFrame()
            date_list = info.get("dates", [])
            date_str  = date_list[0] if date_list else ""
            match_type = info.get("match_type", "t20").lower()
            league_name, country = CRICKET_FORMAT_LEAGUES.get(match_type, ("Cricket", ""))
            outcome = info.get("outcome", {})
            winner = outcome.get("winner", "")
            rows.append({
                "home_team_name": teams[0],
                "away_team_name": teams[1],
                "match_date":     date_str,
                "league_name":    league_name,
                "country":        country,
                "season":         date_str[:4],
                "venue":          info.get("venue", ""),
                "home_score":     1 if winner == teams[0] else (0 if winner else None),
                "away_score":     1 if winner == teams[1] else (0 if winner else None),
                "status":         "finished",
            })
        except Exception as exc:
            logger.warning("Cricsheet parse error (%s): %s", source_file.name, exc)
        return pd.DataFrame(rows)

    def _normalize_kaggle(self, df: pd.DataFrame) -> pd.DataFrame:
        col = df.columns.tolist()
        date_col   = next((c for c in col if "date" in c), None)
        winner_col = next((c for c in col if "winner" in c), None)
        type_col   = next((c for c in col if "match_type" in c or "type" in c), None)
        venue_col  = next((c for c in col if "venue" in c or "city" in c), None)
        season_col = next((c for c in col if "season" in c or "year" in c), None)

        rows = []
        for _, r in df.iterrows():
            match_type = str(r.get(type_col, "odi")).lower() if type_col else "odi"
            league_name, country = CRICKET_FORMAT_LEAGUES.get(match_type, ("Cricket", ""))
            rows.append({
                "home_team_name": str(r.get("team1", "")),
                "away_team_name": str(r.get("team2", "")),
                "match_date":     str(r.get(date_col, "")) if date_col else "",
                "league_name":    league_name,
                "country":        country,
                "season":         str(r.get(season_col, "")) if season_col else "",
                "venue":          str(r.get(venue_col, "")) if venue_col else None,
                "status":         "finished",
            })
        return pd.DataFrame(rows)

    def _normalize_generic(self, df: pd.DataFrame) -> pd.DataFrame:
        col = df.columns.tolist()
        home_col = next((c for c in col if "home" in c or "team1" in c), None)
        away_col = next((c for c in col if "away" in c or "team2" in c), None)
        date_col = next((c for c in col if "date" in c), None)
        if not home_col or not away_col or not date_col:
            return pd.DataFrame()
        rows = []
        for _, r in df.iterrows():
            rows.append({
                "home_team_name": str(r[home_col]),
                "away_team_name": str(r[away_col]),
                "match_date":     str(r[date_col]),
                "league_name":    "Cricket",
                "status":         "finished",
            })
        return pd.DataFrame(rows)
