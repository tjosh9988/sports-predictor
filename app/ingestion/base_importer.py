"""
base_importer.py — Shared base class for all sport historical data importers.

Features
--------
- Recursively discovers CSV and JSON files under a given folder
- Normalises team names via a configurable alias map + fuzzy matching (difflib)
- Upserts into Supabase in configurable batch sizes (default 1000)
- Idempotent: uses ON CONFLICT (match_date, home_team_id, away_team_id) DO NOTHING
- Structured logging with per-file progress
"""

from __future__ import annotations

import json
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any

import difflib
import pandas as pd

logger = logging.getLogger(__name__)


# ─────────────────────────── Team Name Registry ────────────────────────────

class TeamRegistry:
    """
    Holds a canonical name → DB id mapping and resolves aliases / fuzzy matches.
    Populated lazily from the teams table at importer startup.
    """

    def __init__(self, extra_aliases: dict[str, str] | None = None):
        # canonical_name → team_id
        self._id_map: dict[str, int] = {}
        # raw alias → canonical_name (built from aliases + DB short_name)
        self._alias_map: dict[str, str] = {}
        if extra_aliases:
            for alias, canonical in extra_aliases.items():
                self._alias_map[alias.lower().strip()] = canonical.lower().strip()

    def register(self, team_id: int, name: str, short_name: str | None = None) -> None:
        key = name.lower().strip()
        self._id_map[key] = team_id
        self._alias_map[key] = key
        if short_name:
            self._alias_map[short_name.lower().strip()] = key

    def resolve_id(self, raw_name: str) -> int | None:
        """
        Returns DB team_id for *raw_name*.
        1. Exact alias match (fast)
        2. Fuzzy match against all known canonical names (fallback)
        """
        if not raw_name:
            return None
        key = self._normalize(raw_name)

        # 1. Exact alias hit
        canonical = self._alias_map.get(key)
        if canonical and canonical in self._id_map:
            return self._id_map[canonical]

        # 2. Fuzzy match
        all_keys = list(self._id_map.keys())
        matches = difflib.get_close_matches(key, all_keys, n=1, cutoff=0.75)
        if matches:
            logger.debug("Fuzzy matched '%s' → '%s'", raw_name, matches[0])
            self._alias_map[key] = matches[0]   # cache for next time
            return self._id_map[matches[0]]

        return None

    @staticmethod
    def _normalize(name: str) -> str:
        # lowercase, collapse whitespace, drop punctuation noise
        name = name.lower().strip()
        name = re.sub(r"['\-–]", " ", name)
        name = re.sub(r"\s+", " ", name)
        return name


# ─────────────────────────── Base Importer ─────────────────────────────────

class BaseImporter(ABC):
    """
    Abstract base for all sport importers.

    Subclasses must implement:
        sport_slug: str
        load_aliases() -> dict[str, str]
        read_file(path) -> pd.DataFrame
        normalize(df) -> pd.DataFrame  (returns rows matching unified match schema)
    """

    BATCH_SIZE = 1000

    def __init__(self, data_dir: str | Path, supabase_admin):
        self.data_dir = Path(data_dir)
        self.client = supabase_admin   # service-role Supabase client
        self.registry = TeamRegistry(extra_aliases=self.load_aliases())
        self._sport: str | None = None
        self._league_cache: dict[str, int] = {}

    # ── Abstract interface ──────────────────────────────────────

    @property
    @abstractmethod
    def sport_slug(self) -> str: ...

    @abstractmethod
    def load_aliases(self) -> dict[str, str]:
        """Return {raw_name: canonical_name} alias mappings for this sport."""
        ...

    @abstractmethod
    def normalize(self, df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
        """
        Transform a raw DataFrame into rows matching the unified schema:
        Columns expected:
            home_team_name, away_team_name, match_date (ISO str or datetime),
            league_name, season, round (opt), venue (opt),
            home_score (opt), away_score (opt), status
        """
        ...

    # ── Template method: run() ──────────────────────────────────

    def run(self) -> None:
        logger.info("[%s] Starting import from %s", self.sport_slug, self.data_dir)
        self._sport = self._fetch_or_create_sport()
        self._load_team_registry()

        files = self._discover_files()
        logger.info("[%s] Found %d files", self.sport_slug, len(files))

        total_inserted = total_skipped = total_errors = 0

        for file_path in files:
            logger.info("[%s] Processing %s", self.sport_slug, file_path.name)
            try:
                raw_df = self._read_file_safe(file_path)
                if raw_df is None or raw_df.empty:
                    continue
                norm_df = self.normalize(raw_df, file_path)
                if norm_df is None or norm_df.empty:
                    continue
                inserted, skipped = self._upsert_matches(norm_df)
                total_inserted += inserted
                total_skipped  += skipped
            except Exception as exc:
                logger.error("[%s] Error in %s: %s", self.sport_slug, file_path.name, exc, exc_info=True)
                total_errors += 1

        logger.info(
            "[%s] Done — inserted=%d  skipped=%d  file_errors=%d",
            self.sport_slug, total_inserted, total_skipped, total_errors,
        )

    # ── File discovery ──────────────────────────────────────────

    def _discover_files(self) -> list[Path]:
        files: list[Path] = []
        for pattern in ("**/*.csv", "**/*.json"):
            files.extend(self.data_dir.rglob(pattern))
        return sorted(set(files))

    def _read_file_safe(self, path: Path) -> pd.DataFrame | None:
        try:
            if path.suffix.lower() == ".json":
                with path.open(encoding="utf-8", errors="replace") as f:
                    data = json.load(f)
                return pd.DataFrame(data if isinstance(data, list) else [data])
            else:
                # Try utf-8 first, fall back to latin-1
                for enc in ("utf-8", "latin-1", "cp1252"):
                    try:
                        return pd.read_csv(path, encoding=enc, low_memory=False)
                    except UnicodeDecodeError:
                        continue
        except Exception as exc:
            logger.error("Failed to read %s: %s", path, exc)
        return None

    # ── Supabase helpers ────────────────────────────────────────

    def _fetch_or_create_sport(self) -> str:
        res = self.client.table("sports").select("slug").eq("slug", self.sport_slug).single().execute()
        if res.data:
            return res.data["slug"]
        ins = self.client.table("sports").insert({"name": self.sport_slug.replace("-", " ").title(), "slug": self.sport_slug}).execute()
        return ins.data[0]["slug"]

    def _load_team_registry(self) -> None:
        res = (
            self.client.table("teams")
            .select("id, name, short_name")
            .eq("sport", self._sport)
            .execute()
        )
        for row in (res.data or []):
            self.registry.register(row["id"], row["name"], row.get("short_name"))
        logger.info("[%s] Loaded %d teams into registry", self.sport_slug, len(res.data or []))

    def _get_or_create_team(self, name: str) -> int:
        team_id = self.registry.resolve_id(name)
        if team_id:
            return team_id
        # Create new team
        data = {"sport": self._sport, "name": name.strip(), "elo_rating": 1500}
        res = (
            self.client.table("teams")
            .upsert(data, on_conflict="sport,name")
            .execute()
        )
        tid = res.data[0]["id"]
        self.registry.register(tid, name)
        return tid

    def _get_or_create_league(self, name: str, country: str = "") -> int:
        key = f"{name}|{country}"
        if key in self._league_cache:
            return self._league_cache[key]
        data = {"sport": self._sport, "name": name.strip(), "country": country.strip()}
        res = (
            self.client.table("leagues")
            .upsert(data, on_conflict="sport,name,country")
            .execute()
        )
        lid = res.data[0]["id"]
        self._league_cache[key] = lid
        return lid

    def _upsert_matches(self, df: pd.DataFrame) -> tuple[int, int]:
        """Batch-upsert normalised rows, return (inserted, skipped)."""
        inserted = skipped = 0
        rows = df.to_dict(orient="records")

        for batch_start in range(0, len(rows), self.BATCH_SIZE):
            batch = rows[batch_start : batch_start + self.BATCH_SIZE]
            db_rows: list[dict[str, Any]] = []

            for row in batch:
                try:
                    db_row = self._build_db_row(row)
                    if db_row:
                        db_rows.append(db_row)
                except Exception as exc:
                    logger.debug("Skipping row (%s): %s", row, exc)
                    skipped += 1

            if db_rows:
                try:
                    res = (
                        self.client.table("matches")
                        .upsert(db_rows, on_conflict="home_team_id,away_team_id,match_date")
                        .execute()
                    )
                    inserted += len(res.data or [])
                except Exception as exc:
                    logger.error("Batch upsert failed: %s", exc)
                    skipped += len(db_rows)

        return inserted, skipped

    def _build_db_row(self, row: dict[str, Any]) -> dict[str, Any] | None:
        home_name = str(row.get("home_team_name", "") or "").strip()
        away_name = str(row.get("away_team_name", "") or "").strip()
        if not home_name or not away_name:
            return None

        match_date = row.get("match_date")
        if not match_date:
            return None
        if isinstance(match_date, str):
            match_date = self._parse_date(match_date)
        if match_date is None:
            return None

        home_id = self._get_or_create_team(home_name)
        away_id = self._get_or_create_team(away_name)

        league_name = str(row.get("league_name", "Unknown")).strip() or "Unknown"
        country     = str(row.get("country", "")).strip()
        league_id   = self._get_or_create_league(league_name, country)

        db_row: dict[str, Any] = {
            "sport":        self._sport,
            "league_id":    league_id,
            "home_team_id": home_id,
            "away_team_id": away_id,
            "match_date":   match_date.isoformat() if isinstance(match_date, datetime) else str(match_date),
            "status":       str(row.get("status", "finished") or "finished"),
            "season":       str(row.get("season", "") or ""),
            "round":        str(row.get("round", "") or "") or None,
            "venue":        str(row.get("venue", "") or "") or None,
        }
        if row.get("home_score") is not None:
            db_row["home_score"] = self._safe_int(row["home_score"])
        if row.get("away_score") is not None:
            db_row["away_score"] = self._safe_int(row["away_score"])

        return db_row

    # ── Utility helpers ─────────────────────────────────────────

    @staticmethod
    def _parse_date(value: str) -> datetime | None:
        formats = [
            "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y",
            "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
            "%d-%m-%Y", "%d %b %Y", "%B %d, %Y",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(value.strip(), fmt)
            except (ValueError, AttributeError):
                continue
        return None

    @staticmethod
    def _safe_int(val: Any) -> int | None:
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_float(val: Any) -> float | None:
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
        """Drop all-NA rows and reset index."""
        return df.dropna(how="all").drop_duplicates().reset_index(drop=True)
