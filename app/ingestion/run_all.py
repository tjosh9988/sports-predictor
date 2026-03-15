"""
run_all.py — CLI orchestrator for all historical data importers.

Usage:
    python -m app.ingestion.run_all                         # run all sports
    python -m app.ingestion.run_all --sport football        # run one sport
    python -m app.ingestion.run_all --sport nba --sport nfl # run multiple
    python -m app.ingestion.run_all --dry-run               # validate without writing
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from app.database import get_supabase_admin
from app.football_importer import FootballImporter
from app.tennis_importer import TennisImporter
from app.nba_importer import NBAImporter
from app.nfl_importer import NFLImporter
from app.cricket_importer import CricketImporter
from app.nhl_importer import NHLImporter
from app.mlb_importer import MLBImporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingestion.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def build_importers(stats_root: Path, client) -> dict[str, object]:
    """Create all importers mapped by sport slug."""
    return {
        "football": FootballImporter(stats_root / "football",  client),
        "tennis_atp": TennisImporter(stats_root / "tennis_atp", client, tour="atp"),
        "tennis_wta": TennisImporter(stats_root / "tennis_wta", client, tour="wta"),
        "nba":      NBAImporter(stats_root / "nba",      client),
        "nfl":      NFLImporter(stats_root / "nfl",      client),
        "cricket":  CricketImporter(stats_root / "cricket", client),
        "nhl":      NHLImporter(stats_root / "nhl",      client),
        "mlb":      MLBImporter(stats_root / "mlb",      client),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bet Hero historical data importer")
    p.add_argument(
        "--sport",
        action="append",
        dest="sports",
        default=None,
        choices=["football", "tennis_atp", "tennis_wta", "nba", "nfl", "cricket", "nhl", "mlb"],
        help="Sport(s) to import. Repeat for multiple. Omit to run all.",
    )
    p.add_argument(
        "--stats-root",
        default="stats",
        help="Root directory containing sport subfolders (default: ./stats)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate files without writing to Supabase",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    stats_root = Path(args.stats_root).resolve()

    if not stats_root.exists():
        logger.error("Stats directory not found: %s", stats_root)
        sys.exit(1)

    if args.dry_run:
        logger.info("🔍 DRY-RUN mode — no data will be written to Supabase")
        client = None
    else:
        logger.info("Connecting to Supabase…")
        client = get_supabase_admin()

    importers = build_importers(stats_root, client)
    targets = args.sports or list(importers.keys())

    logger.info("⚡ Running importers: %s", ", ".join(targets))

    failed = []
    for sport in targets:
        if sport not in importers:
            logger.warning("Unknown sport '%s' — skipping", sport)
            continue
        importer = importers[sport]
        data_dir = stats_root / sport.replace("tennis_atp", "tennis_atp").replace("tennis_wta", "tennis_wta")
        if not data_dir.exists():
            logger.warning("Data dir not found for %s: %s", sport, data_dir)
            continue
        try:
            if args.dry_run:
                # Discover and validate files only
                files = importer._discover_files()
                logger.info("[%s] DRY-RUN: found %d files", sport, len(files))
            else:
                importer.run()
        except Exception as exc:
            logger.error("Importer failed for %s: %s", sport, exc, exc_info=True)
            failed.append(sport)

    if failed:
        logger.error("❌ Failed sports: %s", ", ".join(failed))
        sys.exit(1)
    else:
        logger.info("✅ All importers complete!")


if __name__ == "__main__":
    main()
