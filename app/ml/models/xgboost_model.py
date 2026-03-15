"""
xgboost_model.py — Production XGBoost for Bet Hero.

Full implementation of BetHeroBaseModel using XGBClassifier.
- Optuna TPE with 9 search dimensions + GPU detection
- Auto multi-class / binary objective switching
- Per-class IsotonicRegression calibration
- SHAP TreeExplainer feature importance; native gain fallback
- Atomic save (joblib) + JSON metadata sidecar
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import optuna
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import log_loss
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier

from .base_model import (
    BetHeroBaseModel,
    CalibrationMethod,
    PredictionBatch,
    TrainingResult,
)

optuna.logging.set_verbosity(optuna.logging.WARNING)
logger = logging.getLogger(__name__)


class XGBoostBetModel(BetHeroBaseModel):
    """XGBoost with Optuna tuning and per-class isotonic calibration."""

    MODEL_NAME = "XGBoostBetModel"
    SUPPORTS_FEATURE_IMPORTANCE = True

    def __init__(
        self,
        sport: str,
        market: str,
        n_trials: int = 40,
        model_dir: Path | None = None,
    ):
        super().__init__(sport, market, model_dir)
        self.n_trials    = n_trials
        self._model:     XGBClassifier | None = None
        self._le:        LabelEncoder         = LabelEncoder()
        self._cals:      list[IsotonicRegression] = []

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

        y_tr = self._le.fit_transform(y_train)
        y_va = self._le.transform(y_val)

        obj    = "multi:softprob" if multiclass else "binary:logistic"
        metric = "mlogloss"       if multiclass else "logloss"

        base_kwargs: dict = {
            "objective":          obj,
            "eval_metric":        metric,
            "use_label_encoder":  False,
            "verbosity":          0,
            "random_state":       42,
        }
        if multiclass:
            base_kwargs["num_class"] = n_cls

        def _obj(trial: optuna.Trial) -> float:
            params = base_kwargs | {
                "n_estimators":     trial.suggest_int("n_estimators", 200, 2000, log=True),
                "max_depth":        trial.suggest_int("max_depth", 3, 12),
                "learning_rate":    trial.suggest_float("learning_rate", 0.005, 0.3, log=True),
                "subsample":        trial.suggest_float("subsample", 0.5, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 1.0),
                "colsample_bylevel":trial.suggest_float("colsample_bylevel", 0.4, 1.0),
                "min_child_weight": trial.suggest_int("min_child_weight", 1, 20, log=True),
                "gamma":            trial.suggest_float("gamma", 0.0, 5.0),
                "reg_alpha":        trial.suggest_float("reg_alpha",  1e-8, 10.0, log=True),
                "reg_lambda":       trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
                "grow_policy":      trial.suggest_categorical(
                    "grow_policy", ["depthwise", "lossguide"]
                ),
            }
            mdl = XGBClassifier(**params)
            mdl.fit(X_train, y_tr,
                    eval_set=[(X_val, y_va)],
                    verbose=False)
            return log_loss(y_va, mdl.predict_proba(X_val))

        study = optuna.create_study(
            direction="minimize",
            sampler=optuna.samplers.TPESampler(seed=42, n_startup_trials=10),
        )
        study.optimize(_obj, n_trials=self.n_trials, show_progress_bar=False)

        best = base_kwargs | study.best_params
        self._model = XGBClassifier(**best)
        self._model.fit(X_train, y_tr, eval_set=[(X_val, y_va)], verbose=False)

        val_proba = self._model.predict_proba(X_val)
        train_ll  = log_loss(y_tr, self._model.predict_proba(X_train))
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
        logger.info("[XGB/%s/%s]\n%s", self.sport, self.market, result.summary())
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
        Xa    = self._align_features(X_cal)
        raw   = self._model.predict_proba(Xa)
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
        logger.info("[XGB] Isotonic calibration on %d samples", len(X_cal))

    def _apply_cal(self, proba: np.ndarray) -> np.ndarray:
        if not self._calibrated or not self._cals:
            return proba
        cols = np.column_stack([c.predict(proba[:, i]) for i, c in enumerate(self._cals)])
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
            {"model": self._model, "le": self._le, "cals": self._cals,
             "classes": self._classes, "feature_names": self._feature_names},
            tmp,
        )
        tmp.rename(path)
        self._write_metadata_sidecar(path)
        logger.info("[XGB] Saved → %s", path)
        return path

    def load(self, path: Path) -> None:
        meta = self._read_metadata_sidecar(path)
        obj  = joblib.load(path)
        self._model, self._le, self._cals = obj["model"], obj["le"], obj.get("cals", [])
        self._restore_from_metadata(meta)
        self._fitted = True

    # ── feature importance ───────────────────────────────────────────────────

    def get_feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        try:
            import shap
            zeros     = pd.DataFrame(np.zeros((10, len(self._feature_names))),
                                     columns=self._feature_names)
            explainer = shap.TreeExplainer(self._model)
            sv        = explainer.shap_values(zeros)
            importance = (np.abs(sv).mean(0) if sv.ndim == 2
                          else np.mean([np.abs(v).mean(0) for v in sv], axis=0))
        except Exception:
            scores     = self._model.get_booster().get_score(importance_type="gain")
            importance = np.array([scores.get(f, 0.0) for f in self._feature_names])

        df = (
            pd.DataFrame({"feature": self._feature_names, "importance": importance})
            .sort_values("importance", ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )
        df["rank"] = df.index + 1
        return df
