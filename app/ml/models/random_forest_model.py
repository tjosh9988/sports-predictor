"""
random_forest_model.py — Production Random Forest for Bet Hero.

Full implementation of BetHeroBaseModel using sklearn RandomForestClassifier.
- Optuna TPE hyperparameter search (n_trials configurable)
- CalibratedClassifierCV isotonic calibration (cv="prefit")
- Native feature importances (Gini impurity) + optional SHAP
- Atomic joblib save with JSON metadata sidecar
- Thread-safe predict via RLock (inherited from base)
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import optuna
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import log_loss

from app.base_model import (
    BetHeroBaseModel,
    CalibrationMethod,
    PredictionBatch,
    TrainingResult,
)

optuna.logging.set_verbosity(optuna.logging.WARNING)
logger = logging.getLogger(__name__)


class RandomForestBetModel(BetHeroBaseModel):
    """Random Forest with Optuna-tuned hyperparameters and isotonic calibration."""

    MODEL_NAME = "RandomForestBetModel"
    SUPPORTS_FEATURE_IMPORTANCE = True

    def __init__(
        self,
        sport: str,
        market: str,
        n_trials: int = 25,
        n_jobs: int = -1,
        model_dir: Path | None = None,
    ):
        super().__init__(sport, market, model_dir)
        self.n_trials = n_trials
        self.n_jobs   = n_jobs
        self._rf:     RandomForestClassifier | None = None
        self._cal:    CalibratedClassifierCV | None = None

    # ── train ────────────────────────────────────────────────────────────────

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val:   pd.DataFrame,
        y_val:   pd.Series,
        **kwargs,
    ) -> TrainingResult:
        t0 = time.monotonic()
        self._classes       = sorted(y_train.unique().tolist())
        self._feature_names = X_train.columns.tolist()

        # ── Optuna tuning ────────────────────────────────────────────────────
        def _obj(trial: optuna.Trial) -> float:
            rf = RandomForestClassifier(
                n_estimators      = trial.suggest_int("n_estimators", 100, 1000),
                max_depth         = trial.suggest_int("max_depth", 4, 30, log=True),
                min_samples_split = trial.suggest_int("min_samples_split", 2, 20),
                min_samples_leaf  = trial.suggest_int("min_samples_leaf", 1, 15),
                max_features      = trial.suggest_categorical(
                    "max_features", ["sqrt", "log2", 0.2, 0.4, 0.6]
                ),
                class_weight = "balanced",
                n_jobs       = self.n_jobs,
                random_state = 42,
            )
            rf.fit(X_train, y_train)
            return log_loss(y_val, rf.predict_proba(X_val), labels=rf.classes_)

        study = optuna.create_study(
            direction="minimize",
            sampler=optuna.samplers.TPESampler(seed=42, n_startup_trials=8),
        )
        study.optimize(_obj, n_trials=self.n_trials, show_progress_bar=False)

        best = study.best_params | {
            "class_weight": "balanced",
            "n_jobs":       self.n_jobs,
            "random_state": 42,
        }
        self._rf = RandomForestClassifier(**best)
        self._rf.fit(X_train, y_train)

        train_ll  = log_loss(y_train, self._rf.predict_proba(X_train), labels=self._classes)
        val_proba = self._rf.predict_proba(X_val)
        metrics   = self._compute_metrics(y_val, val_proba, self._classes)

        self._fitted = True
        result = TrainingResult(
            model_name=self.MODEL_NAME, sport=self.sport, market=self.market,
            n_train=len(X_train), n_val=len(X_val),
            n_features=len(self._feature_names),
            train_log_loss=round(train_ll, 4),
            best_params=best,
            duration_s=round(time.monotonic() - t0, 1),
            **{k: v for k, v in metrics.items() if k != "train_log_loss"},
        )
        result.train_log_loss = round(train_ll, 4)
        self._training_result = result
        logger.info("[RF/%s/%s]\n%s", self.sport, self.market, result.summary())
        return result

    # ── calibrate ────────────────────────────────────────────────────────────

    def calibrate(
        self,
        X_cal: pd.DataFrame,
        y_cal: pd.Series,
        method: str = CalibrationMethod.ISOTONIC,
    ) -> None:
        if not self._fitted:
            raise RuntimeError("Call train() before calibrate()")
        sk_method = "isotonic" if method == CalibrationMethod.ISOTONIC else "sigmoid"
        self._cal = CalibratedClassifierCV(self._rf, method=sk_method, cv="prefit")
        self._cal.fit(self._align_features(X_cal), y_cal)
        self._calibrated = True
        logger.info("[RF] Calibrated (%s) on %d samples", sk_method, len(X_cal))

    # ── predict_proba ────────────────────────────────────────────────────────

    def predict_proba(self, X: pd.DataFrame) -> PredictionBatch:
        if not self._fitted:
            raise RuntimeError("Model not fitted — call train() first")
        with self._predict_lock:
            Xa    = self._align_features(X)
            mdl   = self._cal if self._calibrated else self._rf
            proba = mdl.predict_proba(Xa)
            df    = pd.DataFrame(proba, columns=self._classes)
            return PredictionBatch(
                probabilities=df,
                predicted_class=df.idxmax(axis=1),
                confidence=df.max(axis=1),
                model_name=self.MODEL_NAME,
                sport=self.sport,
                market=self.market,
            )

    # ── save / load ──────────────────────────────────────────────────────────

    def save(self, suffix: str = "") -> Path:
        path = self.model_dir / f"{self.MODEL_NAME}_{self.sport}_{self.market}{suffix}.joblib"
        tmp  = path.with_suffix(".tmp")
        joblib.dump(
            {"rf": self._rf, "cal": self._cal,
             "classes": self._classes, "feature_names": self._feature_names},
            tmp,
        )
        tmp.rename(path)
        self._write_metadata_sidecar(path)
        logger.info("[RF] Saved → %s", path)
        return path

    def load(self, path: Path) -> None:
        meta = self._read_metadata_sidecar(path)
        obj  = joblib.load(path)
        self._rf, self._cal = obj["rf"], obj.get("cal")
        self._restore_from_metadata(meta)
        self._fitted = True
        logger.info("[RF] Loaded from %s", path)

    # ── feature importance ───────────────────────────────────────────────────

    def get_feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        # Try SHAP first; fall back to Gini impurity
        try:
            import shap
            explainer = shap.TreeExplainer(self._rf)
            vals      = explainer.shap_values(
                pd.DataFrame(np.zeros((50, len(self._feature_names))),
                             columns=self._feature_names)
            )
            importance = np.mean([np.abs(v).mean(0) for v in vals], axis=0)
        except Exception:
            importance = self._rf.feature_importances_

        df = (
            pd.DataFrame({"feature": self._feature_names, "importance": importance})
            .sort_values("importance", ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )
        df["rank"] = df.index + 1
        return df
