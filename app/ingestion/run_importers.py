#!/usr/bin/env python3
"""
run_importers.py — One-shot master script to load ALL historical data.

Run this ONCE after your Supabase database is created and your
stats/ folder is populated with CSV/JSON files.

Usage
-----
    python -m app.ingestion.run_importers                       # all sports
    python -m app.ingestion.run_importers --sport football      # one sport
    python -m app.ingestion.run_importers --sport nba --sport nfl
    python -m app.ingestion.run_importers --dry-run             # validate only
    python -m app.ingestion.run_importers --stats-root /data/stats

Output
------
  - Colourised console log with per-sport progress
  - ingestion_report_<timestamp>.json  (summary of every sport)
  - ingestion.log                      (full debug log)
  - Exit code 0 = all succeeded, 1 = one or more sports failed
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

# ─────────────────────────── Logging Setup ─────────────────────────────────

LOG_FILE = "ingestion.log"

def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    fmt   = "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s"
    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
        ],
    )

logger = logging.getLogger(__name__)

# ─────────────────────────── ANSI Colours ──────────────────────────────────

def _c(text: str, code: str) -> str:
    """Wrap text in ANSI colour code (no-op on Windows without colorama)."""
    try:
        import colorama  # optional; silently skip if missing
        colorama.init(autoreset=True)
        return f"{code}{text}\033[0m"
    except ImportError:
        return text

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"


# ─────────────────────────── Result Dataclass ──────────────────────────────

@dataclass
class SportResult:
    sport:       str
    status:      str        # "ok" | "skipped" | "failed"
    inserted:    int = 0
    skipped:     int = 0
    duration_s:  float = 0.0
    error:       str = ""
    file_count:  int = 0


# ─────────────────────────── Import Pipeline ───────────────────────────────

# Sports in execution order: largest/most important first
SPORT_ORDER: list[str] = [
    "football",     # Largest dataset — run first
    "nba",
    "nfl",
    "mlb",
    "nhl",
    "cricket",
    "tennis_atp",
    "tennis_wta",
]

def _build_importer_factory(
    sport: str,
    stats_root: Path,
    client,
) -> Callable | None:
    """Returns a callable that creates and runs the correct importer, or None if dir missing."""
    from app.ingestion.football_importer import FootballImporter
    from app.ingestion.tennis_importer   import TennisImporter
    from app.ingestion.nba_importer      import NBAImporter
    from app.ingestion.nfl_importer      import NFLImporter
    from app.ingestion.cricket_importer  import CricketImporter
    from app.ingestion.nhl_importer      import NHLImporter
    from app.ingestion.mlb_importer      import MLBImporter

    sport_dir_map = {
        "football":   stats_root / "football",
        "nba":        stats_root / "nba",
        "nfl":        stats_root / "nfl",
        "mlb":        stats_root / "mlb",
        "nhl":        stats_root / "nhl",
        "cricket":    stats_root / "cricket",
        "tennis_atp": stats_root / "tennis_atp",
        "tennis_wta": stats_root / "tennis_wta",
    }

    data_dir = sport_dir_map.get(sport)
    if data_dir is None:
        return None

    constructor_map: dict[str, Callable] = {
        "football":   lambda: FootballImporter(data_dir, client),
        "nba":        lambda: NBAImporter(data_dir, client),
        "nfl":        lambda: NFLImporter(data_dir, client),
        "mlb":        lambda: MLBImporter(data_dir, client),
        "nhl":        lambda: NHLImporter(data_dir, client),
        "cricket":    lambda: CricketImporter(data_dir, client),
        "tennis_atp": lambda: TennisImporter(data_dir, client, tour="atp"),
        "tennis_wta": lambda: TennisImporter(data_dir, client, tour="wta"),
    }

    return constructor_map.get(sport), data_dir


# ─────────────────────────── Core Runner ───────────────────────────────────

def _run_sport(
    sport: str,
    stats_root: Path,
    client,
    dry_run: bool,
) -> SportResult:
    """
    Run a single sport importer.
    Returns a SportResult regardless of success/failure.
    """
    result = SportResult(sport=sport, status="ok")
    t0 = time.monotonic()

    # Build importer
    factory_info = _build_importer_factory(sport, stats_root, client)
    if factory_info is None:
        result.status = "skipped"
        result.error  = f"No importer registered for sport '{sport}'"
        return result

    factory_fn, data_dir = factory_info

    if not data_dir.exists():
        result.status = "skipped"
        result.error  = f"Data directory not found: {data_dir}"
        logger.warning("[%s] Skipping — %s", sport, result.error)
        return result

    try:
        importer = factory_fn()

        # Count files before running
        files = importer._discover_files()
        result.file_count = len(files)
        logger.info(
            "[%s] Found %d files in %s",
            _c(sport.upper(), CYAN),
            result.file_count,
            data_dir,
        )

        if result.file_count == 0:
            result.status = "skipped"
            result.error  = "No CSV/JSON files found"
            logger.warning("[%s] No files — skipping", sport)
            return result

        if dry_run:
            logger.info("[%s] DRY-RUN — skipping DB writes", sport)
            result.status = "skipped"
        else:
            # Monkey-patch run() to capture inserted/skipped counts
            _orig_upsert = importer._upsert_matches

            inserted_total = [0]
            skipped_total  = [0]

            def _tracked_upsert(df):
                ins, skp = _orig_upsert(df)
                inserted_total[0] += ins
                skipped_total[0]  += skp
                return ins, skp

            importer._upsert_matches = _tracked_upsert
            importer.run()
            result.inserted = inserted_total[0]
            result.skipped  = skipped_total[0]

    except Exception as exc:
        result.status = "failed"
        result.error  = str(exc)
        logger.error(
            "[%s] FAILED: %s\n%s",
            sport, exc, traceback.format_exc(),
        )

    result.duration_s = time.monotonic() - t0
    return result


# ─────────────────────────── Reporting ─────────────────────────────────────

def _print_summary(results: list[SportResult]) -> None:
    """Pretty-print a final summary table to stdout."""
    print("\n" + "=" * 68)
    print(_c("  BET HERO — HISTORICAL IMPORT SUMMARY", BOLD))
    print("=" * 68)
    print(f"  {'SPORT':<16} {'STATUS':<10} {'FILES':>6} {'INSERTED':>10} {'SKIPPED':>8} {'TIME':>7}")
    print("─" * 68)

    total_inserted = total_skipped = 0

    for r in results:
        colour = GREEN if r.status == "ok" else (YELLOW if r.status == "skipped" else RED)
        print(
            f"  {r.sport:<16}"
            f"  {_c(r.status.upper(), colour):<20}"
            f"  {r.file_count:>4}"
            f"  {r.inserted:>10,}"
            f"  {r.skipped:>8,}"
            f"  {r.duration_s:>5.1f}s"
        )
        if r.error:
            print(f"    {'↳ ' + r.error[:70]}")
        total_inserted += r.inserted
        total_skipped  += r.skipped

    print("─" * 68)
    print(
        f"  {'TOTAL':<16}  {'':10}  {'':4}  {total_inserted:>10,}  {total_skipped:>8,}"
    )
    print("=" * 68 + "\n")


def _save_report(results: list[SportResult], output_dir: Path) -> Path:
    """Save a JSON report for CI/debugging."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_path = output_dir / f"ingestion_report_{ts}.json"
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_inserted": sum(r.inserted for r in results),
        "total_skipped":  sum(r.skipped  for r in results),
        "sports": [asdict(r) for r in results],
    }
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    logger.info("📄 Report saved → %s", report_path)
    return report_path


# ─────────────────────────── CLI ───────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Bet Hero — one-shot historical data importer",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python -m app.ingestion.run_importers\n"
            "  python -m app.ingestion.run_importers --sport football --sport nba\n"
            "  python -m app.ingestion.run_importers --dry-run --verbose\n"
        ),
    )
    p.add_argument(
        "--sport",
        action="append",
        dest="sports",
        default=None,
        choices=SPORT_ORDER,
        metavar="SPORT",
        help=(
            "Import only this sport. Repeat for several.\n"
            f"Choices: {', '.join(SPORT_ORDER)}"
        ),
    )
    p.add_argument(
        "--stats-root",
        default="stats",
        metavar="DIR",
        help="Root directory containing sport sub-folders (default: ./stats).",
    )
    p.add_argument(
        "--report-dir",
        default=".",
        metavar="DIR",
        help="Directory to write JSON report (default: current dir).",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and validate files without writing to Supabase.",
    )
    p.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging.",
    )
    return p.parse_args()


# ─────────────────────────── Main ──────────────────────────────────────────

def main() -> int:
    args = _parse_args()
    _setup_logging(args.verbose)

    stats_root  = Path(args.stats_root).resolve()
    report_dir  = Path(args.report_dir).resolve()
    target_sports = args.sports or SPORT_ORDER

    # ── Validation ─────────────────────────────────────────────
    if not stats_root.exists():
        logger.critical("❌ Stats root directory not found: %s", stats_root)
        logger.critical("   Create it and place your CSV/JSON files inside:")
        logger.critical("   %s/football/*.csv", stats_root)
        logger.critical("   %s/nba/*.csv  etc.", stats_root)
        return 1

    # ── Supabase connection ──────────────────────────────────────
    if args.dry_run:
        client = None
        logger.info(_c("🔍  DRY-RUN mode — Supabase writes are DISABLED", YELLOW))
    else:
        logger.info("Connecting to Supabase …")
        try:
            from app.database import get_supabase_admin, check_supabase_health
            client = get_supabase_admin()
            if not check_supabase_health():
                logger.critical("❌ Supabase health check failed — aborting")
                return 1
            logger.info(_c("✅ Supabase connected", GREEN))
        except Exception as exc:
            logger.critical("❌ Cannot connect to Supabase: %s", exc)
            logger.critical("   Check SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env")
            return 1

    # ── Run importers in order ──────────────────────────────────
    results: list[SportResult] = []
    wall_start = time.monotonic()

    logger.info(
        _c("\n⚡ Starting import for %d sport(s): %s", BOLD),
        len(target_sports),
        ", ".join(target_sports),
    )

    for sport in target_sports:
        print()
        logger.info("─" * 60)
        logger.info("▶  %s", _c(sport.upper(), CYAN))
        logger.info("─" * 60)

        result = _run_sport(sport, stats_root, client, args.dry_run)
        results.append(result)

        if result.status == "ok":
            logger.info(
                _c("✅ [%s] Done — %d inserted, %d skipped in %.1fs", GREEN),
                sport, result.inserted, result.skipped, result.duration_s,
            )
        elif result.status == "skipped":
            logger.warning(_c("⏭  [%s] Skipped — %s", YELLOW), sport, result.error)
        else:
            logger.error(_c("❌ [%s] FAILED — %s", RED), sport, result.error)

    wall_elapsed = time.monotonic() - wall_start

    # ── Summary ─────────────────────────────────────────────────
    _print_summary(results)
    logger.info("⏱  Total wall-clock time: %.1f seconds", wall_elapsed)

    # ── Save report ─────────────────────────────────────────────
    try:
        report_path = _save_report(results, report_dir)
        print(f"  Report: {report_path}")
    except Exception as exc:
        logger.warning("Could not save report: %s", exc)

    # ── Exit code ────────────────────────────────────────────────
    failed = [r for r in results if r.status == "failed"]
    if failed:
        logger.error(
            "❌ %d sport(s) failed: %s",
            len(failed),
            ", ".join(r.sport for r in failed),
        )
        return 1

    logger.info(_c("🏆 All imports completed successfully!", GREEN))
    return 0


# ─────────────────────────── Async Admin Functions ─────────────────────────

async def run_all_importers():
    """
    Asynchronous orchestrator for all sports.
    """
    for sport in SPORT_ORDER:
        await run_single_importer(sport)


async def keep_alive_logger(sport: str):
    """Prints a keep-alive message every 30 seconds."""
    count = 0
    while True:
        try:
            await asyncio.sleep(30)
            count += 30
            print(f"Import still running: {sport} — {count}s elapsed")
        except asyncio.CancelledError:
            break

async def run_single_importer(sport: str):
    """
    Asynchronous orchestrator for a single sport.
    Used by the FastAPI admin endpoint.
    """
    from app.database import get_supabase_admin
    client = get_supabase_admin()
    stats_root = Path("/tmp/stats")
    
    # 1. Build factory to get the importer class
    factory_info = _build_importer_factory(sport, stats_root, client)
    if not factory_info:
        logger.error(f"No importer found for sport: {sport}")
        return

    factory_fn, _ = factory_info
    importer = factory_fn()

    # 2. Trigger download from storage
    if hasattr(importer, "download_from_storage"):
        logger.info(f"Downloading data from Supabase Storage for {sport}...")
        await importer.download_from_storage()
    
    # 3. Run the importer with keep-alive ping
    logger.info(f"Starting ingestion for {sport}...")
    
    # Start the keep-alive logger as a background task
    keep_alive_task = asyncio.create_task(keep_alive_logger(sport))
    
    try:
        # Use to_thread to keep the event loop free for the logger
        await asyncio.to_thread(importer.run)
    except Exception as e:
        logger.error(f"Ingestion failed for {sport}: {e}", exc_info=True)
    finally:
        # Ensure we cancel the logger when ingestion finishes
        keep_alive_task.cancel()
        try:
            await keep_alive_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    sys.exit(main())
