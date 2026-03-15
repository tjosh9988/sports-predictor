"""
training_pipeline_logic.py — Core training and data loading logic.
Internal use by MasterTrainingPipeline.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd

from ..config import settings
from ..database import get_supabase_admin
from .feature_engineer import FeatureEngineer
from .feature_validator import validate_before_training
from .models import (
    BetHeroBaseModel,
    LightGBMBetModel,
    LSTMBetModel,
    NeuralNetworkBetModel,
    RandomForestBetModel,
    StackingEnsemble,
    TrainingResult,
    XGBoostBetModel,
    MARKETS,
    SUPPORTED_SPORTS,
)

logger = logging.getLogger(__name__)

# ─────────────────────────── Constants ─────────────────────────────────────

VAL_FRACTION: float  = 0.15
CAL_FRACTION: float  = 0.10
MIN_ROWS: int        = 200
OPTUNA_TRIALS: int   = 50
MODELS_ROOT: Path    = Path(os.getenv("MODELS_DIR", "models"))

MODEL_REGISTRY: dict[str, type[BetHeroBaseModel]] = {
    "random_forest":    RandomForestBetModel,
    "xgboost":          XGBoostBetModel,
    "lightgbm":         LightGBMBetModel,
    "neural_network":   NeuralNetworkBetModel,
    "lstm":             LSTMBetModel,
    "StackingEnsemble": StackingEnsemble,
}

# ─────────────────────────── Data Classes ──────────────────────────────────

@dataclass
class PipelineConfig:
    sport:      str
    market:     str
    model_name: str
    n_trials:   int              = OPTUNA_TRIALS
    dry_run:    bool             = False
    force:      bool             = False

@dataclass
class PipelineResult:
    sport:            str
    market:           str
    model_name:       str
    status:           str         # "ok" | "skipped" | "failed"
    training_result:  TrainingResult | None = None
    error:            str         = ""
    duration_s:       float       = 0.0
    model_path:       str         = ""
    mlflow_run_id:    str         = ""

# ─────────────────────────── Target Builder ────────────────────────────────

class MarketTargetBuilder:
    @staticmethod
    def build(df: pd.DataFrame, market: str) -> pd.Series:
        hs = df.get("home_score", pd.Series(dtype=float))
        as_ = df.get("away_score", pd.Series(dtype=float))
        total = hs.fillna(0) + as_.fillna(0)

        if market == "match_result":
            return pd.Series(np.where(hs > as_, "H", np.where(hs == as_, "D", "A")), index=df.index)
        elif market == "btts":
            return pd.Series(np.where((hs > 0) & (as_ > 0), "Yes", "No"), index=df.index)
        elif market == "over_25":
            return pd.Series(np.where(total > 2.5, "Over", "Under"), index=df.index)
        elif market == "over_35":
            return pd.Series(np.where(total > 3.5, "Over", "Under"), index=df.index)
        elif market == "asian_handicap":
            return pd.Series(np.where(hs > as_, "Home", "Away"), index=df.index)
        elif market == "cards_over":
            hc, ac = df.get("home_yellow_cards", 0), df.get("away_yellow_cards", 0)
            return pd.Series(np.where((hc + ac) > 3.5, "Over", "Under"), index=df.index)
        elif market == "corners_over":
            hc, ac = df.get("home_corners", 0), df.get("away_corners", 0)
            return pd.Series(np.where((hc + ac) > 9.5, "Over", "Under"), index=df.index)
        return pd.Series(dtype=object)

# ─────────────────────────── Data Loader ───────────────────────────────────

class TrainingDataLoader:
    def __init__(self, supabase_client):
        self.client = supabase_client

    def load(self, sport: str, market: str) -> pd.DataFrame | None:
        sport_res = self.client.table("sports").select("id").eq("slug", sport).single().execute()
        if not sport_res.data: return None
        sport_id = sport_res.data["id"]

        match_res = (self.client.table("matches")
                     .select("id, match_date, home_score, away_score, home_yellow_cards, away_yellow_cards, home_corners, away_corners")
                     .eq("sport_id", sport_id)
                     .eq("status", "finished")
                     .not_.is_("home_score", "null")
                     .order("match_date", desc=False).execute())
        if not match_res.data or len(match_res.data) < MIN_ROWS: return None

        matches = pd.DataFrame(match_res.data)
        matches["match_date"] = pd.to_datetime(matches["match_date"]).dt.tz_localize(None)
        matches["target"] = MarketTargetBuilder.build(matches, market)

        m_ids = matches["id"].tolist()
        feat_res = self.client.table("match_feature_cache").select("match_id, features").in_("match_id", m_ids).execute()
        if not feat_res.data: return None
        
        feat_df = pd.DataFrame([{"match_id": r["match_id"], **r["features"]} for r in feat_res.data]).set_index("match_id")
        df = matches.set_index("id").join(feat_df, how="inner").reset_index().rename(columns={"index": "match_id"})
        return df

# ─────────────────────────── Splitter ──────────────────────────────────────

class TemporalSplitter:
    def split(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        n = len(df)
        nv, nc = int(n * VAL_FRACTION), int(n * CAL_FRACTION)
        nt = n - nv - nc
        return df.iloc[:nt], df.iloc[nt: nt + nc], df.iloc[nt + nc:]

# ─────────────────────────── Logger ────────────────────────────────────────

class MLflowLogger:
    def __init__(self):
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "./mlruns"))

    def log_training(self, result: TrainingResult, config: PipelineConfig, model_path: str) -> str:
        mlflow.set_experiment(f"BetHero/{config.sport}/{config.market}")
        with mlflow.start_run(run_name=config.model_name) as run:
            mlflow.set_tags({"sport": config.sport, "market": config.market, "model": config.model_name})
            mlflow.log_params(result.best_params)
            mlflow.log_metrics({"val_acc": result.val_accuracy, "val_ll": result.val_log_loss, "roi": 0.0})
            if model_path: mlflow.log_artifact(model_path)
            return run.info.run_id

# ─────────────────────────── Trainer ───────────────────────────────────────

class ModelTrainer:
    def __init__(self, config: PipelineConfig, mlflow_logger: MLflowLogger):
        self.config, self.mlflow = config, mlflow_logger
        self.client = get_supabase_admin()

    def run(self) -> PipelineResult:
        cfg = self.config
        loader = TrainingDataLoader(self.client)
        df = loader.load(cfg.sport, cfg.market)
        if df is None: return PipelineResult(cfg.sport, cfg.market, cfg.model_name, "skipped", error="No data")

        split = TemporalSplitter()
        tr, ca, va = split.split(df)
        
        # Identify features
        EXCLUDE = {"match_id", "match_date", "home_score", "away_score", "target"}
        f_cols = [c for c in df.columns if c not in EXCLUDE and pd.api.types.is_numeric_dtype(df[c])]
        
        Xt, xc, xv = tr[f_cols].fillna(0), ca[f_cols].fillna(0), va[f_cols].fillna(0)
        yt, yc, yv = tr["target"], ca["target"], va["target"]

        # Leakage Check
        valid = validate_before_training(Xt, yt, xv, yv, match_dates_train=tr["match_date"], match_dates_val=va["match_date"])
        if not valid.passed: return PipelineResult(cfg.sport, cfg.market, cfg.model_name, "failed", error="Leakage detected")

        # Stacking Ensemble Logic
        if cfg.model_name == "StackingEnsemble":
            # StackingEnsemble requires sub-models. Here we'll treat it as a factory-trained unit.
            # In production, we'd ensure sub-models are trained first.
            model = StackingEnsemble(cfg.sport, cfg.market, model_dir=MODELS_ROOT/cfg.sport/cfg.market)
            model.load_base_models()
            res = model.train(Xt, yt, xv, yv)
            model.calibrate(xc, yc)
            m_path = model.save()
        else:
            cls = MODEL_REGISTRY[cfg.model_name]
            model = cls(cfg.sport, cfg.market, model_dir=MODELS_ROOT/cfg.sport/cfg.market)
            res = model.train(Xt, yt, xv, yv)
            model.calibrate(xc, yc)
            m_path = model.save()

        run_id = self.mlflow.log_training(res, cfg, str(m_path))
        return PipelineResult(cfg.sport, cfg.market, cfg.model_name, "ok", training_result=res, model_path=str(m_path), mlflow_run_id=run_id)
