"""
nhl_importer.py — Imports historical NHL game data.

Handles:
  - hockey-reference.com CSV format
  - Kaggle NHL game dataset (game.csv style)
    (game_id, date_time_GMT, type, away_team_id, home_team_id, goals_home, goals_away)
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from app.base_importer import BaseImporter

logger = logging.getLogger(__name__)

NHL_TEAM_IDS: dict[int, str] = {
    1: "new jersey devils", 2: "new york islanders", 3: "new york rangers",
    4: "philadelphia flyers", 5: "pittsburgh penguins", 6: "boston bruins",
    7: "buffalo sabres", 8: "montreal canadiens", 9: "ottawa senators",
    10: "toronto maple leafs", 12: "carolina hurricanes", 13: "florida panthers",
    14: "tampa bay lightning", 15: "washington capitals", 16: "chicago blackhawks",
    17: "detroit red wings", 18: "nashville predators", 19: "st. louis blues",
    20: "calgary flames", 21: "colorado avalanche", 22: "edmonton oilers",
    23: "vancouver canucks", 24: "anaheim ducks", 25: "dallas stars",
    26: "los angeles kings", 28: "san jose sharks", 29: "columbus blue jackets",
    30: "minnesota wild", 52: "winnipeg jets", 53: "arizona coyotes",
    54: "vegas golden knights", 55: "seattle kraken",
}

NHL_ALIASES: dict[str, str] = {
    "nj":    "new jersey devils",
    "nyi":   "new york islanders",
    "nyr":   "new york rangers",
    "phi":   "philadelphia flyers",
    "pit":   "pittsburgh penguins",
    "bos":   "boston bruins",
    "buf":   "buffalo sabres",
    "mtl":   "montreal canadiens",
    "ott":   "ottawa senators",
    "tor":   "toronto maple leafs",
    "car":   "carolina hurricanes",
    "fla":   "florida panthers",
    "tbl":   "tampa bay lightning",
    "wsh":   "washington capitals",
    "chi":   "chicago blackhawks",
    "det":   "detroit red wings",
    "nsh":   "nashville predators",
    "stl":   "st. louis blues",
    "cgy":   "calgary flames",
    "col":   "colorado avalanche",
    "edm":   "edmonton oilers",
    "van":   "vancouver canucks",
    "ana":   "anaheim ducks",
    "dal":   "dallas stars",
    "lak":   "los angeles kings",
    "sjs":   "san jose sharks",
    "cbj":   "columbus blue jackets",
    "min":   "minnesota wild",
    "wpg":   "winnipeg jets",
    "ari":   "arizona coyotes",
    "vgk":   "vegas golden knights",
    "sea":   "seattle kraken",
    "phx":   "arizona coyotes",    # historical Phoenix
    "atl":   "winnipeg jets",      # historical Atlanta Thrashers
}


class NHLImporter(BaseImporter):

    sport_slug = "nhl"

    def load_aliases(self) -> dict[str, str]:
        return NHL_ALIASES

    def normalize(self, df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
        df = self._clean_df(df)
        df.columns = [c.lower().strip() for c in df.columns]
        col = set(df.columns)

        if "home_team_id" in col and "away_team_id" in col:
            return self._normalize_kaggle(df)
        return self._normalize_hockeyref(df)

    def _normalize_kaggle(self, df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, r in df.iterrows():
            hid = self._safe_int(r.get("home_team_id"))
            aid = self._safe_int(r.get("away_team_id"))
            date_raw = str(r.get("date_time_gmt") or r.get("date_time") or r.get("date", ""))
            rows.append({
                "home_team_name": NHL_TEAM_IDS.get(hid, f"nhl_team_{hid}"),
                "away_team_name": NHL_TEAM_IDS.get(aid, f"nhl_team_{aid}"),
                "match_date":     date_raw[:10],
                "league_name":    "NHL",
                "country":        "USA/Canada",
                "season":         str(r.get("season", "")),
                "home_score":     r.get("goals_home") or r.get("home_goals"),
                "away_score":     r.get("goals_away") or r.get("away_goals"),
                "status":         "finished",
            })
        return pd.DataFrame(rows)

    def _normalize_hockeyref(self, df: pd.DataFrame) -> pd.DataFrame:
        col = df.columns.tolist()
        date_col  = next((c for c in col if "date" in c), None)
        home_col  = next((c for c in col if "home" in c and ("team" in c or c == "home")), None)
        away_col  = next((c for c in col if "visitor" in c or ("away" in c and "team" in c)), None)
        hg_col    = next((c for c in col if c in ("g", "goals", "home_g", "pts")), None)
        ag_col    = next((c for c in col if c in ("g.1", "away_g", "visitor_g")), None)

        if not date_col or not home_col or not away_col:
            logger.warning("NHL: Cannot detect columns: %s", col)
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
                "league_name":    "NHL",
                "country":        "USA/Canada",
                "season":         date_raw[:4] if len(date_raw) >= 4 else "",
                "home_score":     r.get(hg_col) if hg_col else None,
                "away_score":     r.get(ag_col) if ag_col else None,
                "status":         "finished",
            })
        return pd.DataFrame(rows)
