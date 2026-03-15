"""
tennis_importer.py — Imports historical tennis data for ATP and WTA tours.

Handles:
  - Jeff Sackmann / tennis_atp / tennis_wta CSV format
    (the standard open-source tennis datasets)
  - columns: tourney_date, winner_name, loser_name, score,
             surface, tourney_name, round, tourney_level
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from app.base_importer import BaseImporter

logger = logging.getLogger(__name__)

# ATP/WTA tour level → league name
TOURNEY_LEVEL_MAP = {
    "G":   "Grand Slam",
    "M":   "Masters 1000",
    "A":   "ATP 500",
    "D":   "Davis Cup",
    "F":   "Tour Finals",
    "PM":  "WTA Premier Mandatory",
    "P":   "WTA Premier",
    "I":   "WTA International",
    "IT":  "WTA International",
}

TENNIS_ALIASES: dict[str, str] = {
    "djokovic":       "novak djokovic",
    "federer":        "roger federer",
    "nadal":          "rafael nadal",
    "murray":         "andy murray",
    "medvedev":       "daniil medvedev",
    "serena":         "serena williams",
    "serena williams":"serena williams",
    "halep":          "simona halep",
    "swiatek":        "iga swiatek",
    "osaka":          "naomi osaka",
    "wozniacki":      "caroline wozniacki",
    "kvitova":        "petra kvitova",
}


class TennisImporter(BaseImporter):
    """
    Used for both ATP and WTA.  Pass tour="atp" or tour="wta".
    sport_slug is fixed at "tennis" so both share the same DB sport row.
    """

    sport_slug = "tennis"

    def __init__(self, data_dir, supabase_admin, tour: str = "atp"):
        self.tour = tour.lower()
        super().__init__(data_dir, supabase_admin)

    def load_aliases(self) -> dict[str, str]:
        return TENNIS_ALIASES

    def normalize(self, df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
        df = self._clean_df(df)
        df.columns = [c.lower().strip() for c in df.columns]
        col = set(df.columns)

        # Jeff Sackmann format
        if "winner_name" in col and "loser_name" in col:
            return self._normalize_sackmann(df)
        # Generic fallback
        return self._normalize_generic(df)

    def _normalize_sackmann(self, df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        league_suffix = "ATP" if self.tour == "atp" else "WTA"
        for _, r in df.iterrows():
            date_str = str(r.get("tourney_date", "")).strip()
            # tourney_date is YYYYMMDD
            if len(date_str) == 8 and date_str.isdigit():
                date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

            level   = str(r.get("tourney_level", "")).strip()
            t_name  = str(r.get("tourney_name", f"{league_suffix} Tour")).strip()
            league  = f"{TOURNEY_LEVEL_MAP.get(level, t_name)} ({league_suffix})"
            surface = str(r.get("surface", "")).strip()
            rnd     = str(r.get("round", "")).strip()
            score   = str(r.get("score", "")).strip()
            # In tennis: winner = "home", loser = "away" for schema compat
            rows.append({
                "home_team_name": str(r.get("winner_name", "")),
                "away_team_name": str(r.get("loser_name", "")),
                "match_date":     date_str,
                "league_name":    league,
                "country":        "",
                "season":         date_str[:4],
                "round":          rnd,
                "venue":          surface,   # surface as venue proxy
                "home_score":     1,         # winner always scores "1"
                "away_score":     0,
                "status":         "finished",
            })
        return pd.DataFrame(rows)

    def _normalize_generic(self, df: pd.DataFrame) -> pd.DataFrame:
        col = df.columns.tolist()
        p1 = next((c for c in col if "player1" in c or "home" in c or "winner" in c), None)
        p2 = next((c for c in col if "player2" in c or "away" in c or "loser" in c), None)
        dt = next((c for c in col if "date" in c), None)
        if not p1 or not p2 or not dt:
            return pd.DataFrame()

        league_suffix = "ATP" if self.tour == "atp" else "WTA"
        rows = []
        for _, r in df.iterrows():
            rows.append({
                "home_team_name": str(r.get(p1, "")),
                "away_team_name": str(r.get(p2, "")),
                "match_date":     str(r.get(dt, "")),
                "league_name":    f"{league_suffix} Tour",
                "season":         str(r.get(dt, ""))[:4],
                "status":         "finished",
            })
        return pd.DataFrame(rows)
