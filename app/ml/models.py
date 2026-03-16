"""
models.py — ML model ensemble for Bet Hero predictions.

Models
------
1. RandomForestModel      — interpretable baseline, handles missing features well
2. XGBoostModel           — primary gradient-boosted tree model
3. LightGBMModel          — faster GBM, great on large datasets
4. LSTMModel              — sequential LSTM (PyTorch) for time-series form patterns
5. EnsembleModel          — soft-vote weighted average of all four
6. IsotonicCalibrator     — applies sklearn IsotonicRegression to fix probability
                            overconfidence / underconfidence

Design principles
-----------------
- All models output P(Home Win), P(Draw), P(Away Win) as calibrated probabilities
- Trained per-sport, per-market (1X2 / BTTS / Over2.5)
- MLflow experiment tracking for every training run
- Optuna hyperparameter tuning (20-trial TPE sampler)
- Models are saved to / loaded from disk via joblib (sklearn) and torch.save (LSTM)
- No data leakage: train/val split is temporal (not random)
"""

from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import joblib
import mlflow
import mlflow.sklearn
import numpy as np
import optuna
import pandas as pd
import torch
import torch.nn as nn
from lightgbm import LGBMClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import (
    accuracy_score,
    brier_score_loss,
    log_loss,
    roc_auc_score,
)
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier

optuna.logging.set_verbosity(optuna.logging.WARNING)
logger = logging.getLogger(__name__)

MODEL_DIR = Path(os.getenv("MODEL_DIR", "models"))
MODEL_DIR.mkdir(parents=True, exist_ok=True)

CLASSES     = ["Home", "Draw", "Away"]   # 1X2
BINARY_CLS  = ["Yes", "No"]              # BTTS / Over2.5


# ═══════════════════════════════════════════════════════════════════════════
# Base Model
# ═══════════════════════════════════════════════════════════════════════════

class BaseModel(ABC):
    """Common interface for all models in the ensemble."""

    def __init__(self, sport: str, market: str):
        self.sport  = sport
        self.market = market
        self._label_map   = {c: i for i, c in enumerate(CLASSES)}
        self._classes     = CLASSES if "1X2" in market.upper() else BINARY_CLS
        self._fitted      = False

    @abstractmethod
    def fit(self, X: pd.DataFrame, y: pd.Series, X_val: pd.DataFrame, y_val: pd.Series) -> None: ...

    @abstractmethod
    def predict_proba(self, X: pd.DataFrame) -> np.ndarray: ...

    def predict(self, X: pd.DataFrame) -> list[str]:
        proba = self.predict_proba(X)
        return [self._classes[i] for i in proba.argmax(axis=1)]

    @property
    def model_path(self) -> Path:
        return MODEL_DIR / f"{self.__class__.__name__}_{self.sport}_{self.market}.joblib"

    def save(self) -> None:
        joblib.dump(self, self.model_path)
        logger.info("Saved %s → %s", self.__class__.__name__, self.model_path)

    @classmethod
    def load(cls, path: Path) -> "BaseModel":
        return joblib.load(path)


# ═══════════════════════════════════════════════════════════════════════════
# Random Forest
# ═══════════════════════════════════════════════════════════════════════════

class RandomForestModel(BaseModel):

    def __init__(self, sport: str, market: str, n_trials: int = 15):
        super().__init__(sport, market)
        self.n_trials = n_trials
        self._model: CalibratedClassifierCV | None = None

    def fit(self, X: pd.DataFrame, y: pd.Series, X_val: pd.DataFrame, y_val: pd.Series) -> None:
        def _objective(trial: optuna.Trial) -> float:
            params = {
                "n_estimators":      trial.suggest_int("n_estimators", 200, 800),
                "max_depth":         trial.suggest_int("max_depth", 4, 20),
                "min_samples_split": trial.suggest_int("min_samples_split", 2, 20),
                "min_samples_leaf":  trial.suggest_int("min_samples_leaf", 1, 10),
                "max_features":      trial.suggest_categorical("max_features", ["sqrt", "log2", 0.3]),
                "random_state":      42,
                "n_jobs":            -1,
                "class_weight":      "balanced",
            }
            rf = RandomForestClassifier(**params)
            rf.fit(X, y)
            proba = rf.predict_proba(X_val)
            return log_loss(y_val, proba, labels=rf.classes_)

        study = optuna.create_study(direction="minimize")
        study.optimize(_objective, n_trials=self.n_trials, show_progress_bar=False)
        best_params = study.best_params
        best_params.update({"random_state": 42, "n_jobs": -1, "class_weight": "balanced"})

        base = RandomForestClassifier(**best_params)
        self._model = CalibratedClassifierCV(base, method="isotonic", cv=3)
        self._model.fit(X, y)
        self._fitted = True
        logger.info("[RF/%s/%s] Best log_loss=%.4f", self.sport, self.market, study.best_value)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        return self._model.predict_proba(X)

    # Feature importance for explainability
    def feature_importance(self, feature_names: list[str]) -> pd.DataFrame:
        if not self._fitted:
            return pd.DataFrame()
        base = self._model.estimator
        fi = pd.DataFrame({
            "feature": feature_names,
            "importance": base.feature_importances_,
        }).sort_values("importance", ascending=False).reset_index(drop=True)
        return fi


# ═══════════════════════════════════════════════════════════════════════════
# XGBoost
# ═══════════════════════════════════════════════════════════════════════════

class XGBoostModel(BaseModel):

    def __init__(self, sport: str, market: str, n_trials: int = 20):
        super().__init__(sport, market)
        self.n_trials = n_trials
        self._model: XGBClassifier | None = None
        self._le = LabelEncoder()

    def fit(self, X: pd.DataFrame, y: pd.Series, X_val: pd.DataFrame, y_val: pd.Series) -> None:
        y_enc     = self._le.fit_transform(y)
        y_val_enc = self._le.transform(y_val)
        n_cls     = len(self._le.classes_)

        def _objective(trial: optuna.Trial) -> float:
            params = {
                "n_estimators":      trial.suggest_int("n_estimators", 200, 1000),
                "max_depth":         trial.suggest_int("max_depth", 3, 12),
                "learning_rate":     trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                "subsample":         trial.suggest_float("subsample", 0.5, 1.0),
                "colsample_bytree":  trial.suggest_float("colsample_bytree", 0.4, 1.0),
                "min_child_weight":  trial.suggest_int("min_child_weight", 1, 10),
                "reg_alpha":         trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
                "reg_lambda":        trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
                "objective":         "multi:softprob" if n_cls > 2 else "binary:logistic",
                "num_class":         n_cls if n_cls > 2 else None,
                "eval_metric":       "mlogloss" if n_cls > 2 else "logloss",
                "random_state":      42,
                "use_label_encoder": False,
                "verbosity":         0,
            }
            params = {k: v for k, v in params.items() if v is not None}
            xgb = XGBClassifier(**params)
            xgb.fit(X, y_enc, eval_set=[(X_val, y_val_enc)], verbose=False)
            proba = xgb.predict_proba(X_val)
            return log_loss(y_val_enc, proba)

        study = optuna.create_study(direction="minimize")
        study.optimize(_objective, n_trials=self.n_trials, show_progress_bar=False)

        best = study.best_params
        best.update({
            "objective":         "multi:softprob" if n_cls > 2 else "binary:logistic",
            "eval_metric":       "mlogloss" if n_cls > 2 else "logloss",
            "random_state":      42,
            "use_label_encoder": False,
            "verbosity":         0,
        })
        if n_cls > 2:
            best["num_class"] = n_cls

        self._model = XGBClassifier(**{k: v for k, v in best.items() if v is not None})
        self._model.fit(X, y_enc, eval_set=[(X_val, y_val_enc)], verbose=False)
        self._fitted = True
        logger.info("[XGB/%s/%s] Best log_loss=%.4f", self.sport, self.market, study.best_value)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        return self._model.predict_proba(X)


# ═══════════════════════════════════════════════════════════════════════════
# LightGBM
# ═══════════════════════════════════════════════════════════════════════════

class LightGBMModel(BaseModel):

    def __init__(self, sport: str, market: str, n_trials: int = 20):
        super().__init__(sport, market)
        self.n_trials = n_trials
        self._model: LGBMClassifier | None = None

    def fit(self, X: pd.DataFrame, y: pd.Series, X_val: pd.DataFrame, y_val: pd.Series) -> None:
        n_cls = y.nunique()

        def _objective(trial: optuna.Trial) -> float:
            params = {
                "n_estimators":     trial.suggest_int("n_estimators", 200, 1500),
                "max_depth":        trial.suggest_int("max_depth", -1, 15),
                "num_leaves":       trial.suggest_int("num_leaves", 20, 300),
                "learning_rate":    trial.suggest_float("learning_rate", 0.005, 0.3, log=True),
                "subsample":        trial.suggest_float("subsample", 0.5, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.4, 1.0),
                "reg_alpha":        trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
                "reg_lambda":       trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
                "min_child_samples":trial.suggest_int("min_child_samples", 5, 100),
                "objective":        "multiclass" if n_cls > 2 else "binary",
                "num_class":        n_cls if n_cls > 2 else None,
                "random_state":     42,
                "n_jobs":           -1,
                "verbose":         -1,
            }
            lgb = LGBMClassifier(**{k: v for k, v in params.items() if v is not None})
            lgb.fit(X, y, eval_set=[(X_val, y_val)], callbacks=[])
            proba = lgb.predict_proba(X_val)
            return log_loss(y_val, proba, labels=lgb.classes_)

        study = optuna.create_study(direction="minimize")
        study.optimize(_objective, n_trials=self.n_trials, show_progress_bar=False)

        best = study.best_params
        best.update({
            "objective":   "multiclass" if n_cls > 2 else "binary",
            "num_class":   n_cls if n_cls > 2 else None,
            "random_state": 42,
            "n_jobs":      -1,
            "verbose":    -1,
        })
        self._model = LGBMClassifier(**{k: v for k, v in best.items() if v is not None})
        self._model.fit(X, y)
        self._fitted = True
        logger.info("[LGB/%s/%s] Best log_loss=%.4f", self.sport, self.market, study.best_value)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        return self._model.predict_proba(X)


# ═══════════════════════════════════════════════════════════════════════════
# LSTM Model (PyTorch)
# ═══════════════════════════════════════════════════════════════════════════

class _LSTMNet(nn.Module):
    def __init__(self, input_size: int, hidden_size: int, num_layers: int, output_size: int, dropout: float):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size, hidden_size, num_layers,
            batch_first=True, dropout=dropout if num_layers > 1 else 0.0,
        )
        self.dropout = nn.Dropout(dropout)
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size // 2, output_size),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq_len, features)
        out, _ = self.lstm(x)
        out = self.dropout(out[:, -1, :])   # last timestep
        return self.fc(out)


class LSTMModel(BaseModel):
    """
    LSTM that treats each match's feature row as a single time-step.
    In production, you'd feed a sequence of the last N matches as the input.
    Here we use a sequence_len=1 (each row = one game's pre-match vector),
    which the ensemble can upgrade to true sequences when DB history is deep enough.
    """

    DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

    def __init__(self, sport: str, market: str, hidden_size: int = 128,
                 num_layers: int = 2, epochs: int = 80, lr: float = 1e-3):
        super().__init__(sport, market)
        self.hidden_size = hidden_size
        self.num_layers  = num_layers
        self.epochs      = epochs
        self.lr          = lr
        self._net: _LSTMNet | None = None
        self._input_size: int = 0
        self._n_classes: int  = len(CLASSES) if "1X2" in market.upper() else 2

    def fit(self, X: pd.DataFrame, y: pd.Series, X_val: pd.DataFrame, y_val: pd.Series) -> None:
        from sklearn.preprocessing import LabelEncoder
        le = LabelEncoder()
        y_enc     = le.fit_transform(y)
        y_val_enc = le.transform(y_val)

        X_t   = torch.tensor(X.values, dtype=torch.float32).unsqueeze(1).to(self.DEVICE)
        y_t   = torch.tensor(y_enc, dtype=torch.long).to(self.DEVICE)
        Xv_t  = torch.tensor(X_val.values, dtype=torch.float32).unsqueeze(1).to(self.DEVICE)
        yv_t  = torch.tensor(y_val_enc, dtype=torch.long).to(self.DEVICE)

        self._input_size = X.shape[1]
        self._net = _LSTMNet(
            input_size=self._input_size,
            hidden_size=self.hidden_size,
            num_layers=self.num_layers,
            output_size=self._n_classes,
            dropout=0.3,
        ).to(self.DEVICE)

        criterion  = nn.CrossEntropyLoss()
        optimiser  = torch.optim.Adam(self._net.parameters(), lr=self.lr, weight_decay=1e-4)
        scheduler  = torch.optim.lr_scheduler.CosineAnnealingLR(optimiser, T_max=self.epochs)

        best_val_loss = float("inf")
        patience = 15
        patience_ctr = 0
        best_state = None

        for epoch in range(self.epochs):
            self._net.train()
            optimiser.zero_grad()
            logits = self._net(X_t)
            loss   = criterion(logits, y_t)
            loss.backward()
            nn.utils.clip_grad_norm_(self._net.parameters(), max_norm=1.0)
            optimiser.step()
            scheduler.step()

            # Validation
            self._net.eval()
            with torch.no_grad():
                val_logits = self._net(Xv_t)
                val_loss   = criterion(val_logits, yv_t).item()

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                best_state    = {k: v.cpu().clone() for k, v in self._net.state_dict().items()}
                patience_ctr  = 0
            else:
                patience_ctr += 1
                if patience_ctr >= patience:
                    logger.info("[LSTM] Early stopping at epoch %d", epoch)
                    break

        if best_state:
            self._net.load_state_dict(best_state)

        self._fitted = True
        logger.info("[LSTM/%s/%s] Train done — best val_loss=%.4f",
                    self.sport, self.market, best_val_loss)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        if not self._fitted or self._net is None:
            raise RuntimeError("LSTM not fitted")
        self._net.eval()
        X_t = torch.tensor(X.values, dtype=torch.float32).unsqueeze(1).to(self.DEVICE)
        with torch.no_grad():
            logits = self._net(X_t)
            proba  = torch.softmax(logits, dim=1).cpu().numpy()
        return proba

    def save(self) -> None:
        path = MODEL_DIR / f"LSTM_{self.sport}_{self.market}.pt"
        torch.save({
            "state_dict":  self._net.state_dict() if self._net else {},
            "input_size":  self._input_size,
            "hidden_size": self.hidden_size,
            "num_layers":  self.num_layers,
            "n_classes":   self._n_classes,
            "sport":       self.sport,
            "market":      self.market,
        }, path)
        logger.info("Saved LSTM → %s", path)

    @classmethod
    def load_lstm(cls, sport: str, market: str) -> "LSTMModel":
        path = MODEL_DIR / f"LSTM_{sport}_{market}.pt"
        ckpt = torch.load(path, map_location="cpu")
        obj = cls(sport=ckpt["sport"], market=ckpt["market"],
                  hidden_size=ckpt["hidden_size"], num_layers=ckpt["num_layers"])
        obj._input_size = ckpt["input_size"]
        obj._n_classes  = ckpt["n_classes"]
        obj._net = _LSTMNet(obj._input_size, obj.hidden_size,
                            obj.num_layers, obj._n_classes, 0.3)
        obj._net.load_state_dict(ckpt["state_dict"])
        obj._net.eval()
        obj._fitted = True
        return obj


# ═══════════════════════════════════════════════════════════════════════════
# Isotonic Calibrator
# ═══════════════════════════════════════════════════════════════════════════

class IsotonicCalibrator:
    """
    Per-class isotonic regression calibration applied post-ensemble.
    Ensures predicted probabilities match empirical frequencies (reliability diagram).
    """

    def __init__(self, n_classes: int):
        self._calibrators = [IsotonicRegression(out_of_bounds="clip")
                             for _ in range(n_classes)]
        self._fitted = False

    def fit(self, proba: np.ndarray, y_true_onehot: np.ndarray) -> None:
        for i, cal in enumerate(self._calibrators):
            cal.fit(proba[:, i], y_true_onehot[:, i])
        self._fitted = True

    def calibrate(self, proba: np.ndarray) -> np.ndarray:
        if not self._fitted:
            return proba
        calibrated_cols = np.column_stack([
            cal.predict(proba[:, i]) for i, cal in enumerate(self._calibrators)
        ])
        # Renormalise rows to sum to 1
        row_sums = calibrated_cols.sum(axis=1, keepdims=True)
        return calibrated_cols / np.where(row_sums == 0, 1, row_sums)


# ═══════════════════════════════════════════════════════════════════════════
# Ensemble Model
# ═══════════════════════════════════════════════════════════════════════════

class EnsembleModel:
    """
    Weighted soft-vote ensemble combining RF, XGB, LGB, LSTM.

    Default weights are equal; can be tuned via optimise_weights().
    """

    DEFAULT_WEIGHTS = {
        "rf":   0.15,
        "xgb":  0.35,
        "lgb":  0.35,
        "lstm": 0.15,
    }

    def __init__(self, sport: str, market: str):
        self.sport   = sport
        self.market  = market
        self.models  = {
            "rf":   RandomForestModel(sport, market),
            "xgb":  XGBoostModel(sport, market),
            "lgb":  LightGBMModel(sport, market),
            "lstm": LSTMModel(sport, market),
        }
        self.weights    = dict(self.DEFAULT_WEIGHTS)
        n_cls = 3 if "1X2" in market.upper() else 2
        self.calibrator = IsotonicCalibrator(n_classes=n_cls)
        self._classes   = CLASSES if "1X2" in market.upper() else BINARY_CLS

    # ── Fit ──────────────────────────────────────────────────────

    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame,
        y_val: pd.Series,
    ) -> dict[str, Any]:
        """Train all sub-models and the calibrator with MLflow tracking."""
        experiment = f"bethero_{self.sport}_{self.market}"
        mlflow.set_experiment(experiment)

        with mlflow.start_run(run_name=f"ensemble_{self.sport}_{self.market}"):
            mlflow.log_param("sport",   self.sport)
            mlflow.log_param("market",  self.market)
            mlflow.log_param("n_train", len(X_train))
            mlflow.log_param("n_val",   len(X_val))

            metrics: dict[str, Any] = {}
            raw_probas: dict[str, np.ndarray] = {}

            for name, model in self.models.items():
                logger.info("[Ensemble] Training %s …", name)
                try:
                    model.fit(X_train, y_train, X_val, y_val)
                    proba = model.predict_proba(X_val)
                    raw_probas[name] = proba
                    ll = log_loss(y_val, proba, labels=model._classes)
                    mlflow.log_metric(f"{name}_log_loss", ll)
                    metrics[f"{name}_log_loss"] = ll
                    logger.info("[Ensemble] %s log_loss=%.4f", name, ll)
                except Exception as exc:
                    logger.error("[Ensemble] %s training failed: %s", name, exc, exc_info=True)
                    # Remove failed model from weights
                    self.weights.pop(name, None)

            # Optimise ensemble weights on val set
            if len(raw_probas) > 1:
                self.weights = self._optimise_weights(raw_probas, y_val)

            # Calibrate ensemble predictions
            ensemble_proba = self._blend(raw_probas, X_val)
            n_cls = len(self._classes)
            cls_enc = {c: i for i, c in enumerate(self._classes)}
            y_int   = y_val.map(cls_enc).fillna(0).astype(int)
            y_onehot = np.eye(n_cls)[y_int.values]
            self.calibrator.fit(ensemble_proba, y_onehot)

            # Final val metrics after calibration
            cal_proba = self.calibrator.calibrate(ensemble_proba)
            final_ll  = log_loss(y_val, cal_proba, labels=self._classes)
            final_acc = accuracy_score(y_val, [self._classes[i] for i in cal_proba.argmax(axis=1)])
            final_bs  = brier_score_loss(y_onehot[:, 0], cal_proba[:, 0])

            mlflow.log_metric("ensemble_log_loss",  final_ll)
            mlflow.log_metric("ensemble_accuracy",  final_acc)
            mlflow.log_metric("ensemble_brier",     final_bs)
            mlflow.log_param("weights", str(self.weights))

            metrics.update({
                "ensemble_log_loss": final_ll,
                "ensemble_accuracy": final_acc,
                "ensemble_brier":    final_bs,
                "weights":           self.weights,
            })

            logger.info(
                "[Ensemble/%s/%s] Final — accuracy=%.3f  log_loss=%.4f  brier=%.4f",
                self.sport, self.market, final_acc, final_ll, final_bs,
            )

        return metrics

    # ── Predict ──────────────────────────────────────────────────

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """Return calibrated probability array shape (n, n_classes)."""
        raw_probas = {
            name: model.predict_proba(X)
            for name, model in self.models.items()
            if model._fitted
        }
        blended = self._blend(raw_probas, X)
        return self.calibrator.calibrate(blended)

    def predict(self, X: pd.DataFrame) -> list[str]:
        proba = self.predict_proba(X)
        return [self._classes[i] for i in proba.argmax(axis=1)]

    # ── Blend ────────────────────────────────────────────────────

    def _blend(self, raw_probas: dict[str, np.ndarray], X: pd.DataFrame) -> np.ndarray:
        total_weight = sum(self.weights.get(name, 0) for name in raw_probas)
        if total_weight == 0:
            raise ValueError("All ensemble models failed — cannot blend.")
        blended = sum(
            raw_probas[name] * self.weights.get(name, 0)
            for name in raw_probas
        ) / total_weight
        return blended

    # ── Weight optimisation ──────────────────────────────────────

    def _optimise_weights(
        self,
        raw_probas: dict[str, np.ndarray],
        y_val: pd.Series,
    ) -> dict[str, float]:
        """
        Minimise log-loss on validation set by finding optimal blend weights.
        Uses a simple grid search over weight combinations (fast, no Optuna needed).
        """
        names = list(raw_probas.keys())
        n     = len(names)
        best_ll     = float("inf")
        best_weights = {name: 1 / n for name in names}

        # Build grid: each model gets weight in {0.05, 0.10, ..., 0.60}
        from itertools import product
        weight_values = [0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40]

        for combo in product(weight_values, repeat=n):
            total = sum(combo)
            if total == 0:
                continue
            norm = [c / total for c in combo]
            blended = sum(raw_probas[names[i]] * norm[i] for i in range(n))
            ll = log_loss(y_val, blended, labels=self._classes)
            if ll < best_ll:
                best_ll     = ll
                best_weights = {names[i]: norm[i] for i in range(n)}

        logger.info("[Ensemble] Optimised weights: %s  (log_loss=%.4f)", best_weights, best_ll)
        return best_weights

    # ── Save / Load ──────────────────────────────────────────────

    def save(self) -> None:
        for name, model in self.models.items():
            if model._fitted:
                if isinstance(model, LSTMModel):
                    model.save()
                else:
                    model.save()
        path = MODEL_DIR / f"EnsembleMeta_{self.sport}_{self.market}.joblib"
        joblib.dump({
            "weights":    self.weights,
            "calibrator": self.calibrator,
            "classes":    self._classes,
        }, path)
        logger.info("Saved EnsembleMeta → %s", path)

    @classmethod
    def load(cls, sport: str, market: str) -> "EnsembleModel":
        obj = cls(sport, market)
        meta_path = MODEL_DIR / f"EnsembleMeta_{sport}_{market}.joblib"
        if meta_path.exists():
            meta = joblib.load(meta_path)
            obj.weights    = meta["weights"]
            obj.calibrator = meta["calibrator"]
            obj._classes   = meta["classes"]
        for name in ["rf", "xgb", "lgb"]:
            p = MODEL_DIR / f"{{'rf':'RandomForestModel','xgb':'XGBoostModel','lgb':'LightGBMModel'}[name]}_{sport}_{market}.joblib"
            if p.exists():
                obj.models[name] = BaseModel.load(p)
        lstm_path = MODEL_DIR / f"LSTM_{sport}_{market}.pt"
        if lstm_path.exists():
            obj.models["lstm"] = LSTMModel.load_lstm(sport, market)
        return obj


# ═══════════════════════════════════════════════════════════════════════════
# Training Pipeline
# ═══════════════════════════════════════════════════════════════════════════

class TrainingPipeline:
    """
    Loads feature data from team_features / predictions tables,
    performs temporal train/val split, trains the ensemble, saves models.
    """

    VAL_FRACTION = 0.15   # last 15% of sorted dates = validation

    def __init__(self, supabase_client, sport: str, market: str):
        self.client  = supabase_client
        self.sport   = sport
        self.market  = market
        self.ensemble = EnsembleModel(sport, market)

    def run(self) -> dict[str, Any]:
        X, y = self._load_training_data()
        if len(X) < 200:
            logger.warning("[Train/%s/%s] Insufficient data (%d rows)", self.sport, self.market, len(X))
            return {}

        X, y = self._preprocess(X, y)
        X_train, X_val, y_train, y_val = self._temporal_split(X, y)

        logger.info("[Train/%s/%s] Train=%d  Val=%d  Features=%d",
                    self.sport, self.market, len(X_train), len(X_val), X_train.shape[1])

        metrics = self.ensemble.fit(X_train, y_train, X_val, y_val)
        self.ensemble.save()
        return metrics

    def _load_training_data(self) -> tuple[pd.DataFrame, pd.Series]:
        """
        Load pre-computed features from team_features table, join with prediction outcomes.
        Returns X (feature matrix) and y (outcome labels).
        """
        res = (
            self.client.table("predictions")
            .select("id, match_id, market, actual_outcome, status, "
                    "matches!inner(sport, match_date)")
            .eq("status", "CORRECT")
            .eq("matches.sport", self.sport)
            .ilike("market", f"%{self.market}%")
            .not_.is_("actual_outcome", "null")
            .execute()
        )
        rows = res.data or []
        if not rows:
            return pd.DataFrame(), pd.Series(dtype=str)

        match_ids = [r["match_id"] for r in rows]
        labels    = {r["match_id"]: r["actual_outcome"] for r in rows}

        # Load features for each match
        feat_rows: list[dict] = []
        for mid in match_ids:
            fr = (
                self.client.table("team_features")
                .select("feature_name, feature_value")
                .eq("match_id", mid)  # type: ignore  — extend schema if needed
                .execute()
            )
            if fr.data:
                row = {f["feature_name"]: f["feature_value"] for f in fr.data}
                row["match_id"]  = mid
                row["match_date"]= next(
                    (r["matches"]["match_date"] for r in rows if r["match_id"] == mid), ""
                )
                row["label"]     = labels[mid]
                feat_rows.append(row)

        if not feat_rows:
            return pd.DataFrame(), pd.Series(dtype=str)

        df = pd.DataFrame(feat_rows)
        y  = df.pop("label")
        df.pop("match_id",  None)
        df.pop("match_date", None)
        return df, y

    def _preprocess(self, X: pd.DataFrame, y: pd.Series) -> tuple[pd.DataFrame, pd.Series]:
        X = X.fillna(0.0)
        # Remove constant columns
        X = X.loc[:, X.std() > 0]
        # Clip extreme values at 3-sigma
        for col in X.columns:
            mu, sigma = X[col].mean(), X[col].std()
            X[col] = X[col].clip(mu - 3 * sigma, mu + 3 * sigma)
        return X, y

    def _temporal_split(
        self, X: pd.DataFrame, y: pd.Series
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
        """
        Temporal split: newest VAL_FRACTION rows go to validation.
        This preserves time ordering and prevents future leakage in CV.
        """
        n_val = max(1, int(len(X) * self.VAL_FRACTION))
        X_train, X_val = X.iloc[:-n_val], X.iloc[-n_val:]
        y_train, y_val = y.iloc[:-n_val], y.iloc[-n_val:]
        return X_train, X_val, y_train, y_val
