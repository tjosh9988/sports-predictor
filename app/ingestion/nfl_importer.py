"""
nfl_importer.py — Imports historical NFL game data.

Handles:
  - Pro Football Reference CSV format
    (Date, Home, Visitor, PtsW, PtsL, home_team indicator)
  - Kaggle NFL dataset (spreads / game logs)
    (schedule_date, home_team, away_team, home_score, away_score)
  - nflverse / nflfastR style CSVs
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from app.base_importer import BaseImporter

logger = logging.getLogger(__name__)

NFL_ALIASES: dict[str, str] = {
    # Standard abbreviation → full name
    "ne":    "new england patriots",
    "kc":    "kansas city chiefs",
    "sf":    "san francisco 49ers",
    "49ers": "san francisco 49ers",
    "nyg":   "new york giants",
    "nyj":   "new york jets",
    "dal":   "dallas cowboys",
    "gb":    "green bay packers",
    "sea":   "seattle seahawks",
    "den":   "denver broncos",
    "lar":   "los angeles rams",
    "lac":   "los angeles chargers",
    "buf":   "buffalo bills",
    "phi":   "philadelphia eagles",
    "pit":   "pittsburgh steelers",
    "bal":   "baltimore ravens",
    "cle":   "cleveland browns",
    "cin":   "cincinnati bengals",
    "ind":   "indianapolis colts",
    "jax":   "jacksonville jaguars",
    "ten":   "tennessee titans",
    "hou":   "houston texans",
    "mia":   "miami dolphins",
    "tb":    "tampa bay buccaneers",
    "atl":   "atlanta falcons",
    "car":   "carolina panthers",
    "no":    "new orleans saints",
    "min":   "minnesota vikings",
    "chi":   "chicago bears",
    "det":   "detroit lions",
    "was":   "washington commanders",
    "ari":   "arizona cardinals",
    "lv":    "las vegas raiders",
    "oak":   "las vegas raiders",   # historical
    "sd":    "los angeles chargers", # historical
    "stl":   "los angeles rams",    # historical
}


class NFLImporter(BaseImporter):
    sport_slug = "nfl"

    async def download_from_storage(self):
        """Downloads all files for nfl from Supabase Storage"""
        import os
        folder = "nfl"
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
        return NFL_ALIASES

    def normalize(self, df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
        df = self._clean_df(df)
        df.columns = [c.lower().strip() for c in df.columns]
        col = set(df.columns)

        if "schedule_date" in col or "gameday" in col:
            return self._normalize_nflverse(df)
        if "pts_win" in col or "ptsw" in col:
            return self._normalize_pfr(df)
        return self._normalize_generic(df)

    def _normalize_nflverse(self, df: pd.DataFrame) -> pd.DataFrame:
        col = df.columns.tolist()
        date_col  = next((c for c in col if c in ("schedule_date", "gameday", "game_date", "date")), None)
        home_col  = next((c for c in col if c in ("home_team", "team_home")), None)
        away_col  = next((c for c in col if c in ("away_team", "team_away")), None)
        hscore    = next((c for c in col if c in ("home_score", "pts_home")), None)
        ascore    = next((c for c in col if c in ("away_score", "pts_away")), None)
        season_col = next((c for c in col if "season" in c or "year" in c), None)
        week_col   = next((c for c in col if c in ("week", "schedule_week")), None)

        if not date_col or not home_col or not away_col:
            return pd.DataFrame()

        rows = []
        for _, r in df.iterrows():
            rows.append({
                "home_team_name": str(r.get(home_col, "")),
                "away_team_name": str(r.get(away_col, "")),
                "match_date":     str(r.get(date_col, "")),
                "league_name":    "NFL",
                "country":        "USA",
                "season":         str(r.get(season_col, "")) if season_col else "",
                "round":          f"Week {int(r[week_col])}" if week_col and pd.notna(r.get(week_col)) else None,
                "home_score":     r.get(hscore) if hscore else None,
                "away_score":     r.get(ascore) if ascore else None,
                "status":         "finished",
            })
        return pd.DataFrame(rows)

    def _normalize_pfr(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pro Football Reference export — contains winner/loser, not home/away directly."""
        col = df.columns.tolist()
        # PFR uses @ symbol to indicate away team; "winner" is the winner
        winner_col = next((c for c in col if "winner" in c), None)
        loser_col  = next((c for c in col if "loser" in c), None)
        date_col   = next((c for c in col if "date" in c or "week" in c), None)
        pts_w      = next((c for c in col if c in ("pts_win", "ptsw", "pts_w")), None)
        pts_l      = next((c for c in col if c in ("pts_loss", "ptsl", "pts_l")), None)
        loc_col    = next((c for c in col if c in ("gamesite", "location", "gametype")), None)

        if not winner_col or not loser_col:
            return self._normalize_generic(df)

        rows = []
        for _, r in df.iterrows():
            winner = str(r.get(winner_col, ""))
            loser  = str(r.get(loser_col, ""))
            # PFR marks away game with @ prefix
            is_away = "@" in str(r.get(loc_col, "")) if loc_col else False
            home = loser if is_away else winner
            away = winner if is_away else loser
            rows.append({
                "home_team_name": home,
                "away_team_name": away,
                "match_date":     str(r.get(date_col, "")),
                "league_name":    "NFL",
                "country":        "USA",
                "season":         str(r.get("year", "")) or "",
                "home_score":     r.get(pts_l) if is_away else r.get(pts_w),
                "away_score":     r.get(pts_w) if is_away else r.get(pts_l),
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
                "home_team_name": str(r.get(home_col, "")),
                "away_team_name": str(r.get(away_col, "")),
                "match_date":     str(r.get(date_col, "")),
                "league_name":    "NFL",
                "country":        "USA",
                "status":         "finished",
            })
        return pd.DataFrame(rows)
