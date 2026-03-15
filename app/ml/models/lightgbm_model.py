"""
lightgbm_model.py — Production LightGBM for Bet Hero.

Full BetHeroBaseModel implementation using LGBMClassifier.
- Optuna TPE search: num_leaves, max_depth, learning_rate, subsample,
  colsample_bytree, min_child_samples, reg_alpha, reg_lambda, boosting_type
- LightGBM native early stopping callbacks
- Per-class IsotonicRegression calibration
- SHAP TreeExplainer; native split-gain fallback
- Atomic joblib save with JSON metadata sidecar
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import optuna
from lightgbm import LGBMClassifier, early_stopping, log_evaluation
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import log_loss

from app.base_model import (
    BetHeroBaseModel,
    CalibrationMethod,
    PredictionBatch,
    TrainingResult,
)

optuna.logging.set_verbosity(optuna.logging.WARNING)
logger = logging.getLogger(__name__)


class LightGBMBetModel(BetHeroBaseModel):
    """LightGBM with num_leaves Optuna tuning and per-class isotonic calibration."""

    MODEL_NAME = "LightGBMBetModel"
    SUPPORTS_FEATURE_IMPORTANCE = True

    def __init__(
        self,
        sport: str,
        market: str,
        n_trials: int = 40,
        model_dir: Path | None = None,
    ):
        super().__init__(sport, market, model_dir)
        self.n_trials = n_trials
        self._model: LGBMClassifier | None        = None
        self._cals:  list[IsotonicRegression]     = []

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
        n_cls               = len(self._classes)
        multiclass          = n_cls > 2

        base: dict = {
            "objective":    "multiclass" if multiclass else "binary",
            "metric":       "multi_logloss" if multiclass else "binary_logloss",
            "num_class":    n_cls if multiclass else None,
            "random_state": 42,
            "n_jobs":      -1,
            "verbose":     -1,
        }

        def _obj(trial: optuna.Trial) -> float:
            p = base | {
                "n_estimators":      trial.suggest_int("n_estimators", 200, 3000, log=True),
                "num_leaves":        trial.suggest_int("num_leaves", 20, 500, log=True),
                "max_depth":         trial.suggest_int("max_depth", -1, 20),
                "learning_rate":     trial.suggest_float("learning_rate", 0.003, 0.3, log=True),
                "subsample":         trial.suggest_float("subsample", 0.5, 1.0),
                "subsample_freq":    trial.suggest_int("subsample_freq", 0, 5),
                "colsample_bytree":  trial.suggest_float("colsample_bytree", 0.4, 1.0),
                "reg_alpha":         trial.suggest_float("reg_alpha",  1e-8, 10.0, log=True),
                "reg_lambda":        trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
                "min_child_samples": trial.suggest_int("min_child_samples", 5, 100, log=True),
                "boosting_type":     trial.suggest_categorical(
                    "boosting_type", ["gbdt", "dart"]
                ),
                "path_smooth":       trial.suggest_float("path_smooth", 0.0, 1.0),
            }
            p = {k: v for k, v in p.items() if v is not None}
            mdl = LGBMClassifier(**p)
            mdl.fit(
                X_train, y_train,
                eval_set=[(X_val, y_val)],
                callbacks=[early_stopping(50, verbose=False), log_evaluation(-1)],
            )
            return log_loss(y_val, mdl.predict_proba(X_val), labels=mdl.classes_)

        study = optuna.create_study(
            direction="minimize",
            sampler=optuna.samplers.TPESampler(seed=42, n_startup_trials=10),
        )
        study.optimize(_obj, n_trials=self.n_trials, show_progress_bar=False)

        best = {k: v for k, v in (base | study.best_params).items() if v is not None}
        self._model = LGBMClassifier(**best)
        self._model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            callbacks=[early_stopping(50, verbose=False), log_evaluation(-1)],
        )

        val_proba = self._model.predict_proba(X_val)
        train_ll  = log_loss(y_train, self._model.predict_proba(X_train), labels=self._classes)
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
        logger.info("[LGB/%s/%s]\n%s", self.sport, self.market, result.summary())
        return result

    # ── calibrate ────────────────────────────────────────────────────────────

    def calibrate(
        self,
        X_cal: pd.DataFrame,
        y_cal: pd.Series,
        method: str = CalibrationMethod.ISOTONIC,
    ) -> None:
        if not self._fitted:
            raise RuntimeError("Call train() first")
        raw   = self._model.predict_proba(self._align_features(X_cal))
        n_cls = len(self._classes)
        le_m  = {c: i for i, c in enumerate(self._classes)}
        y_enc = y_cal.map(le_m).fillna(0).astype(int).values
        y_ohe = np.eye(n_cls)[y_enc]
        self._cals = []
        for i in range(n_cls):
            iso = IsotonicRegression(out_of_bounds="clip")
            iso.fit(raw[:, i], y_ohe[:, i])
            self._cals.append(iso)
        self._calibrated = True
        logger.info("[LGB] Calibrated on %d samples", len(X_cal))

    def _apply_cal(self, p: np.ndarray) -> np.ndarray:
        if not self._calibrated or not self._cals:
            return p
        cols = np.column_stack([c.predict(p[:, i]) for i, c in enumerate(self._cals)])
        s    = cols.sum(axis=1, keepdims=True)
        return cols / np.where(s == 0, 1, s)

    # ── predict_proba ────────────────────────────────────────────────────────

    def predict_proba(self, X: pd.DataFrame) -> PredictionBatch:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        with self._predict_lock:
            raw   = self._model.predict_proba(self._align_features(X))
            proba = self._apply_cal(raw)
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
            {"model": self._model, "cals": self._cals,
             "classes": self._classes, "feature_names": self._feature_names},
            tmp,
        )
        tmp.rename(path)
        self._write_metadata_sidecar(path)
        logger.info("[LGB] Saved → %s", path)
        return path

    def load(self, path: Path) -> None:
        meta = self._read_metadata_sidecar(path)
        obj  = joblib.load(path)
        self._model = obj["model"]
        self._cals  = obj.get("cals", [])
        self._restore_from_metadata(meta)
        self._fitted = True

    # ── feature importance ───────────────────────────────────────────────────

    def get_feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        try:
            import shap
            zeros     = pd.DataFrame(np.zeros((20, len(self._feature_names))),
                                     columns=self._feature_names)
            explainer = shap.TreeExplainer(self._model)
            sv        = explainer.shap_values(zeros)
            importance = (np.mean([np.abs(v).mean(0) for v in sv], axis=0)
                          if isinstance(sv, list) else np.abs(sv).mean(0))
        except Exception:
            importance = self._model.feature_importances_

        df = (
            pd.DataFrame({"feature": self._feature_names, "importance": importance})
            .sort_values("importance", ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )
        df["rank"] = df.index + 1
        return df
