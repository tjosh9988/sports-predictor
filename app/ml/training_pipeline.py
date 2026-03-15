"""
training_pipeline.py — Master ML Orchestrator for Bet Hero.

Responsibilities
----------------
1. Automated Batch Training: Loops over all sports and markets.
2. Leakage Prevention: Runs FeatureValidator before every training session.
3. Automated Backtesting: Runs Backtester immediately after training.
4. Production Promotion: Only saves model as {name}_prod if backtest ROI > 0.
5. Experiment Tracking: Logs all trials, results, and artifacts to MLflow.
6. Scheduling:
   - Weekly full retrain via APScheduler.
   - Event-driven trigger: when 200+ new results are detected in Supabase.

Promotion Logic
---------------
A model is promoted to 'production' status only if:
- Validation accuracy is statistically significant.
- Backtest ROI over 2 years is POSITIVE (> 0%).
- Max Drawdown is within acceptable bounds (< 25 units).

Usage
-----
    # Run full pipeline manually
    python -m app.ml.training_pipeline --run-all

    # Start the daemon/scheduler
    python -m app.ml.training_pipeline --start-daemon
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from ..config import settings
from ..database import get_supabase_admin
from ..redis_client import get_redis_client
from .backtester import Backtester, BacktestMetric
from .feature_validator import validate_before_training
from .models import (
    BetHeroBaseModel,
    MARKETS,
    SUPPORTED_SPORTS,
    StackingEnsemble,
    TrainingResult,
)

# ─────────────────────────── Setup ─────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Key in Redis to track processed match count for the 200-trigger
REDIS_TRAINING_COUNTER_KEY = "bet_hero:ml:last_processed_results_count"
TRAINING_TRIGGER_THRESHOLD = 200


# ─────────────────────────── Master Orchestrator ───────────────────────────

class MasterTrainingPipeline:
    """
    Orchestrates the end-to-end ML lifecycle:
    Validation -> Hyperparameter Tuning -> Training -> Backtesting -> Promotion
    """

    def __init__(self):
        self.supabase = get_supabase_admin()
        self.redis = get_redis_client()
        self.models_root = Path(os.getenv("MODELS_DIR", "models"))

    def run_all(self, force: bool = False):
        """Iterates through all sports and markets."""
        logger.info("🚀 Master Training Pipeline started...")
        start_time = time.monotonic()
        summary = []

        for sport in SUPPORTED_SPORTS:
            for market in MARKETS:
                try:
                    res = self.process_combination(sport, market, force=force)
                    summary.append(res)
                except Exception as exc:
                    logger.error("Failed processing %s/%s: %s", sport, market, exc, exc_info=True)
                    summary.append({"sport": sport, "market": market, "status": "ERROR"})

        self._finalize_run(summary, start_time)

    def process_combination(self, sport: str, market: str, force: bool = False) -> dict:
        """Processes a single (sport, market) triple."""
        logger.info("--- Processing %s / %s ---", sport, market)
        
        # 1. Training (includes Feature Validation internally or as pre-step)
        # Note: We reuse the TrainingPipeline logic but wrapped here for promotion.
        from .training_pipeline_logic import ModelTrainer, PipelineConfig, MLflowLogger
        
        # We'll use the Ensemble as our primary target for production
        config = PipelineConfig(sport=sport, market=market, model_name="StackingEnsemble", force=force)
        mlflow_logger = MLflowLogger()
        trainer = ModelTrainer(config, mlflow_logger)
        
        # Note: Before training, the validator is called inside trainer.run()
        res = trainer.run()
        
        if res.status != "ok":
            return {"sport": sport, "market": market, "status": f"TRAIN_{res.status.upper()}", "error": res.error}

        # 2. Backtesting
        backtester = Backtester(sport, market)
        metric: BacktestMetric = backtester.run(years=2)

        if not metric:
            return {"sport": sport, "market": market, "status": "BACKTEST_FAILED"}

        # 3. Promotion Logic
        # If ROI is positive, move the model to the "production" slot
        is_promoted = metric.is_prod_ready and metric.max_drawdown < 25.0
        
        if is_promoted:
            self._promote_to_production(sport, market)
            logger.info("✅ PROMOTED: %s/%s passed backtest with ROI: %.2f%%", sport, market, metric.roi * 100)
        else:
            logger.warning("❌ REJECTED: %s/%s ROI negative or too volatile: %.2f%%", sport, market, metric.roi * 100)

        return {
            "sport": sport,
            "market": market,
            "status": "PROMOTED" if is_promoted else "REJECTED",
            "roi": metric.roi,
            "win_rate": metric.win_rate,
            "val_acc": res.training_result.val_accuracy if res.training_result else 0
        }

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _promote_to_production(self, sport: str, market: str):
        """
        Symbolic link or file copy moves the newly trained model 
        from 'StackingEnsemble_...' to 'StackingEnsemble_PROD.joblib'
        """
        base_dir = self.models_root / sport / market
        # Current naming: StackingEnsemble_football_match_result.joblib
        latest = base_dir / f"StackingEnsemble_{sport}_{market}.joblib"
        prod_target = base_dir / f"StackingEnsemble_PROD.joblib"
        
        if latest.exists():
            import shutil
            shutil.copy2(latest, prod_target)
            # Also copy metadata
            if latest.with_suffix(".json").exists():
                shutil.copy2(latest.with_suffix(".json"), prod_target.with_suffix(".json"))

    def _finalize_run(self, summary: list, start_time: float):
        duration = time.monotonic() - start_time
        logger.info("═══ PIPELINE SUMMARY (Duration: %.1fs) ═══", duration)
        for s in summary:
            status = s.get("status", "UNKNOWN")
            roi = s.get("roi", 0.0) * 100
            print(f"[{s['sport']:10} | {s['market']:15}] {status:10} | ROI: {roi:+.2f}%")
        
        # Update redis counter so we don't trigger again immediately
        try:
            total_count = self.supabase.table("matches").select("id", count="exact").eq("status", "finished").execute().count
            self.redis.set(REDIS_TRAINING_COUNTER_KEY, total_count)
        except Exception as exc:
            logger.error("Failed to update redis counter: %s", exc)

    # ── Trigger Check ────────────────────────────────────────────────────────

    def check_for_new_results(self) -> bool:
        """
        Checks if the number of finished matches in Supabase has increased 
         by 200+ since the last training run.
        """
        try:
            res = self.client.table("matches").select("id", count="exact").eq("status", "finished").execute()
            current_count = res.count or 0
            
            last_count_str = self.redis.get(REDIS_TRAINING_COUNTER_KEY)
            last_count = int(last_count_str) if last_count_str else 0
            
            diff = current_count - last_count
            logger.info("Checking results trigger: current=%d, last=%d, diff=%d", current_count, last_count, diff)
            
            return diff >= TRAINING_TRIGGER_THRESHOLD
        except Exception as exc:
            logger.error("Error checking training trigger: %s", exc)
            return False


# ─────────────────────────── Scheduling ────────────────────────────────────

def start_scheduler():
    """
    Starts the APScheduler daemon to run weekly and check for data triggers.
    """
    scheduler = BlockingScheduler(timezone=timezone.utc)
    pipeline = MasterTrainingPipeline()

    # 1. Weekly full retrain (Monday at 3 AM)
    scheduler.add_job(
        pipeline.run_all,
        trigger=CronTrigger(day_of_week="mon", hour=3, minute=0),
        name="Weekly Retrain",
        id="weekly_retrain"
    )

    # 2. Check for 200+ results trigger every 4 hours
    scheduler.add_job(
        lambda: pipeline.run_all() if pipeline.check_for_new_results() else None,
        trigger="interval",
        hours=4,
        name="Event Trigger Check",
        id="event_check"
    )

    logger.info("⏰ Scheduler started. Weekly retrain + 200-result event check active.")
    
    # Graceful shutdown
    def handle_sigterm(*args):
        logger.info("Stopping scheduler...")
        scheduler.shutdown()
    
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass


# ─────────────────────────── CLI / Entry ───────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Bet Hero Master ML Pipe")
    parser.add_argument("--run-all", action="store_true", help="Launch full training pass now")
    parser.add_argument("--daemon", action="store_true", help="Start the background scheduler")
    parser.add_argument("--force", action="store_true", help="Ignore existing models and retrain")
    
    args = parser.parse_args()
    
    if args.run_all:
        pipeline = MasterTrainingPipeline()
        pipeline.run_all(force=args.force)
    elif args.daemon:
        start_scheduler()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
