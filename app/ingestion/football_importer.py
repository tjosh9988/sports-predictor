"""
football_importer.py — Imports historical football data from:
  - stats/football/ (handles European leagues Kaggle dataset,
    Football-Data.co.uk CSVs, and any generic CSV with home/away columns)

Supported column families:
  - Football-Data.co.uk  : Date, HomeTeam, AwayTeam, FTHG, FTAG, Div, Season
  - Kaggle European Soccer: match_api_id, date, home_team_api_id (resolved via team lookup)
  - Generic              : home_team/away_team or home/away + date columns

The importer auto-detects the format and dispatches accordingly.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from app.base_importer import BaseImporter

logger = logging.getLogger(__name__)


# ── League name map: Football-Data.co.uk division codes → readable names ──
DIVISION_MAP: dict[str, tuple[str, str]] = {
    "E0":  ("Premier League",        "England"),
    "E1":  ("Championship",          "England"),
    "E2":  ("League One",            "England"),
    "E3":  ("League Two",            "England"),
    "EC":  ("Conference",            "England"),
    "SC0": ("Scottish Premiership",  "Scotland"),
    "SC1": ("Scottish Championship", "Scotland"),
    "D1":  ("Bundesliga",            "Germany"),
    "D2":  ("2. Bundesliga",         "Germany"),
    "SP1": ("La Liga",               "Spain"),
    "SP2": ("La Liga 2",             "Spain"),
    "I1":  ("Serie A",               "Italy"),
    "I2":  ("Serie B",               "Italy"),
    "F1":  ("Ligue 1",               "France"),
    "F2":  ("Ligue 2",               "France"),
    "N1":  ("Eredivisie",            "Netherlands"),
    "B1":  ("First Division A",      "Belgium"),
    "P1":  ("Primeira Liga",         "Portugal"),
    "T1":  ("Süper Lig",             "Turkey"),
    "G1":  ("Super League",          "Greece"),
}

# Common team aliases (expand as needed from your actual data)
FOOTBALL_ALIASES: dict[str, str] = {
    "man utd":               "manchester united",
    "man united":            "manchester united",
    "man city":              "manchester city",
    "spurs":                 "tottenham hotspur",
    "tottenham":             "tottenham hotspur",
    "wolves":                "wolverhampton wanderers",
    "brighton":              "brighton & hove albion",
    "sheffield utd":         "sheffield united",
    "sheffield wednesddays": "sheffield wednesday",
    "nottm forest":          "nottingham forest",
    "qpr":                   "queens park rangers",
    "west brom":             "west bromwich albion",
    "palace":                "crystal palace",
    "brentford fc":          "brentford",
    "newcastle":             "newcastle united",
    "leicester":             "leicester city",
    "norwich":               "norwich city",
    "watford fc":            "watford",
    "west ham":              "west ham united",
    "atletico":              "atletico madrid",
    "atl madrid":            "atletico madrid",
    "real":                  "real madrid",
    "barca":                 "barcelona",
    "fc barcelona":          "barcelona",
    "fcb":                   "barcelona",
    "inter":                 "inter milan",
    "inter milan":           "inter milan",
    "ac milan":              "milan",
    "psv":                   "psv eindhoven",
    "ajax":                  "ajax amsterdam",
    "dortmund":              "borussia dortmund",
    "bvb":                   "borussia dortmund",
    "gladbach":              "borussia monchengladbach",
    "rb leipzig":            "rasenballsport leipzig",
    "leverkusen":            "bayer leverkusen",
}


class FootballImporter(BaseImporter):

    sport_slug = "football"

    def load_aliases(self) -> dict[str, str]:
        return FOOTBALL_ALIASES

    def normalize(self, df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
        df = self._clean_df(df)
        cols = {c.lower() for c in df.columns}

        # ── Format detection ───────────────────────────────────
        if "hometeam" in cols and "awayteam" in cols:
            return self._normalize_football_data(df)
        elif "home_team_api_id" in cols:
            return self._normalize_kaggle(df)
        else:
            return self._normalize_generic(df)

    # ── Football-Data.co.uk format ──────────────────────────────
    def _normalize_football_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [c.lower().strip() for c in df.columns]
        rows = []
        for _, r in df.iterrows():
            date_val = r.get("date") or r.get("dateymd")
            div = str(r.get("div", "")).strip()
            league_name, country = DIVISION_MAP.get(div, ("Unknown League", ""))
            rows.append({
                "home_team_name": str(r.get("hometeam", "")),
                "away_team_name": str(r.get("awayteam", "")),
                "match_date":     str(date_val),
                "league_name":    league_name,
                "country":        country,
                "season":         str(r.get("season", "")),
                "home_score":     r.get("fthg"),
                "away_score":     r.get("ftag"),
                "status":         "finished",
            })
        return pd.DataFrame(rows)

    # ── Kaggle European Soccer DB format ───────────────────────
    def _normalize_kaggle(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [c.lower().strip() for c in df.columns]
        # Kaggle stores team API IDs — we'll use them as names with prefix
        # so they still get created as unique teams in the DB
        rows = []
        for _, r in df.iterrows():
            rows.append({
                "home_team_name": f"team_{r.get('home_team_api_id', 'unk')}",
                "away_team_name": f"team_{r.get('away_team_api_id', 'unk')}",
                "match_date":     str(r.get("date", "")),
                "league_name":    str(r.get("league_id", "Unknown")),
                "country":        str(r.get("country_id", "")),
                "season":         str(r.get("season", "")),
                "home_score":     r.get("home_team_goal"),
                "away_score":     r.get("away_team_goal"),
                "status":         "finished",
            })
        return pd.DataFrame(rows)

    # ── Generic / fallback format ──────────────────────────────
    def _normalize_generic(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [c.lower().strip() for c in df.columns]
        col = df.columns.tolist()

        home_col  = next((c for c in col if "home" in c and "team" in c), None) or \
                    next((c for c in col if c in ("home", "home_team")), None)
        away_col  = next((c for c in col if "away" in c and "team" in c), None) or \
                    next((c for c in col if c in ("away", "away_team")), None)
        date_col  = next((c for c in col if "date" in c), None)
        hg_col    = next((c for c in col if c in ("hg", "home_goals", "home_score", "fthg", "ghome")), None)
        ag_col    = next((c for c in col if c in ("ag", "away_goals", "away_score", "ftag", "gaway")), None)
        league_col = next((c for c in col if "league" in c or "div" in c or "comp" in c), None)
        season_col = next((c for c in col if "season" in c or "year" in c), None)

        if not home_col or not away_col or not date_col:
            logger.warning("Cannot normalise file — missing home/away/date columns: %s", col)
            return pd.DataFrame()

        rows = []
        for _, r in df.iterrows():
            rows.append({
                "home_team_name": str(r.get(home_col, "")),
                "away_team_name": str(r.get(away_col, "")),
                "match_date":     str(r.get(date_col, "")),
                "league_name":    str(r.get(league_col, "Football")) if league_col else "Football",
                "season":         str(r.get(season_col, "")) if season_col else "",
                "home_score":     r.get(hg_col) if hg_col else None,
                "away_score":     r.get(ag_col) if ag_col else None,
                "status":         "finished",
            })
        return pd.DataFrame(rows)
