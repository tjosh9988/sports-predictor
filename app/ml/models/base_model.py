"""
base_model.py — Abstract base class for all Bet Hero ML models.

Every concrete model (RandomForest, XGBoost, LightGBM, LSTM, etc.)
must inherit from BetHeroBaseModel and implement the abstract methods.

Design contract
---------------
- train()              Fit on training data; stores internal state
- predict_proba()      Return calibrated probability matrix (n_samples, n_classes)
- calibrate()          Post-hoc probability calibration (isotonic / Platt)
- save() / load()      Persist to / restore from disk (joblib + optional torch.save)
- backtest()           Walk-forward validation on a full historical DataFrame
- get_feature_importance()  SHAP or native importances as a ranked DataFrame

Design decisions
----------------
- All methods receive and return pd.DataFrame / pd.Series to enforce
  named columns (prevents silent column-order bugs).
- predict_proba() always returns a DataFrame with class-name columns,
  not a plain numpy array, so downstream code can reference by name.
- Calibration is pluggable: isotonic (default), sigmoid (Platt), or
  temperature scaling (useful for neural nets).
- Backtest uses a rolling-origin (expanding window) walk-forward split
  so there is zero leakage between train and test periods.
- Persistence is versioned: model files embed sport, market, and a
  semver so mismatched files raise a clear error instead of silently
  producing wrong predictions.
- All methods log timing, dataset sizes, and key metrics via the
  standard Python logger (no side-effects, easy to redirect).
- Thread-safe: predict_proba() acquires a read-lock so multiple
  FastAPI workers can share a loaded model without corruption.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, ClassVar

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ─────────────────────────── Model version sentinel ───────────────────────

MODEL_SCHEMA_VERSION = "1.0.0"   # bump when the base class contract changes


# ─────────────────────────── Result Dataclasses ────────────────────────────

@dataclass
class TrainingResult:
    """Returned by train().  Rich summary of what the model learned."""
    model_name:       str
    sport:            str
    market:           str
    n_train:          int
    n_val:            int
    n_features:       int
    train_log_loss:   float
    val_log_loss:     float
    val_accuracy:     float
    val_brier_score:  float
    val_roc_auc:      float             # OvR macro for multi-class
    best_params:      dict              = field(default_factory=dict)
    feature_importances: pd.DataFrame  = field(default_factory=pd.DataFrame)
    duration_s:       float            = 0.0
    trained_at:       str              = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    calibrated:       bool             = False
    notes:            list[str]        = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"{'─'*55}",
            f"  {self.model_name}  [{self.sport} / {self.market}]",
            f"{'─'*55}",
            f"  Train rows    : {self.n_train:,}",
            f"  Val rows      : {self.n_val:,}",
            f"  Features      : {self.n_features}",
            f"  Val accuracy  : {self.val_accuracy:.3f}",
            f"  Val log-loss  : {self.val_log_loss:.4f}",
            f"  Val Brier     : {self.val_brier_score:.4f}",
            f"  Val ROC-AUC   : {self.val_roc_auc:.4f}   (OvR macro)",
            f"  Calibrated    : {'Yes' if self.calibrated else 'No'}",
            f"  Duration      : {self.duration_s:.1f}s",
            f"{'─'*55}",
        ]
        for note in self.notes:
            lines.append(f"  ℹ  {note}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["feature_importances"] = self.feature_importances.to_dict() if not self.feature_importances.empty else {}
        return d


@dataclass
class BacktestResult:
    """
    Returned by backtest().
    Each fold represents one walk-forward window.
    """
    model_name:     str
    sport:          str
    market:         str
    n_folds:        int
    fold_metrics:   list[dict[str, float]]    = field(default_factory=list)
    # Aggregated stats across all folds
    mean_accuracy:  float = 0.0
    mean_log_loss:  float = 0.0
    mean_brier:     float = 0.0
    mean_roc_auc:   float = 0.0
    std_accuracy:   float = 0.0
    roi_simulated:  float = 0.0    # unit-stake ROI if betting every prediction above threshold
    sharpe_ratio:   float = 0.0

    def summary(self) -> str:
        lines = [
            f"{'─'*55}",
            f"  Backtest  {self.model_name}  [{self.sport} / {self.market}]",
            f"  Folds: {self.n_folds}",
            f"{'─'*55}",
            f"  Mean accuracy  : {self.mean_accuracy:.3f} ± {self.std_accuracy:.3f}",
            f"  Mean log-loss  : {self.mean_log_loss:.4f}",
            f"  Mean Brier     : {self.mean_brier:.4f}",
            f"  Mean ROC-AUC   : {self.mean_roc_auc:.4f}",
            f"  Simulated ROI  : {self.roi_simulated:+.2%}",
            f"  Sharpe ratio   : {self.sharpe_ratio:.3f}",
            f"{'─'*55}",
        ]
        return "\n".join(lines)


@dataclass
class PredictionBatch:
    """Returned by predict_proba(). Wraps a probability DataFrame with metadata."""
    probabilities:   pd.DataFrame          # columns = class names, rows = samples
    predicted_class: pd.Series             # argmax class label per row
    confidence:      pd.Series             # max probability per row
    model_name:      str = ""
    sport:           str = ""
    market:          str = ""
    generated_at:    str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def top_confidence(self, n: int = 10) -> pd.DataFrame:
        """Return the n most confident predictions."""
        df = self.probabilities.copy()
        df["predicted"] = self.predicted_class
        df["confidence"] = self.confidence
        return df.sort_values("confidence", ascending=False).head(n)


# ─────────────────────────── Calibration strategies ────────────────────────

class CalibrationMethod:
    ISOTONIC    = "isotonic"
    PLATT       = "sigmoid"
    TEMPERATURE = "temperature"


# ─────────────────────────── Abstract Base Model ───────────────────────────

class BetHeroBaseModel(ABC):
    """
    Abstract base class that every Bet Hero ML model must implement.

    Concrete subclasses
    -------------------
    - RandomForestBetModel
    - XGBoostBetModel
    - LightGBMBetModel
    - LSTMBetModel
    - EnsembleBetModel

    Thread safety
    -------------
    predict_proba() acquires self._predict_lock (RLock) so multiple FastAPI
    workers can share one loaded model without data races.
    """

    # Subclasses set these as class attributes
    MODEL_NAME: ClassVar[str] = "BetHeroBaseModel"
    SUPPORTS_FEATURE_IMPORTANCE: ClassVar[bool] = True

    def __init__(self, sport: str, market: str, model_dir: Path | None = None):
        """
        Parameters
        ----------
        sport      : e.g. "football", "nba", "tennis"
        market     : e.g. "1X2", "BTTS", "Over2.5"
        model_dir  : directory to save/load artifacts (default: ./models)
        """
        self.sport       = sport
        self.market      = market
        self.model_dir   = Path(model_dir) if model_dir else Path("models")
        self.model_dir.mkdir(parents=True, exist_ok=True)

        self._fitted          = False
        self._calibrated      = False
        self._classes: list[str] = []
        self._feature_names:  list[str] = []
        self._predict_lock    = threading.RLock()
        self._training_result: TrainingResult | None = None
        self._metadata:       dict[str, Any] = {
            "schema_version": MODEL_SCHEMA_VERSION,
            "sport":          sport,
            "market":         market,
            "model_name":     self.MODEL_NAME,
        }

    # ══════════════════════════════════════════════════════════════════════
    # Abstract methods — must be implemented by every subclass
    # ══════════════════════════════════════════════════════════════════════

    @abstractmethod
    def train(
        self,
        X_train:   pd.DataFrame,
        y_train:   pd.Series,
        X_val:     pd.DataFrame,
        y_val:     pd.Series,
        **kwargs,
    ) -> TrainingResult:
        """
        Fit the model on training data, validate on X_val/y_val.

        Contract
        --------
        - Must set self._fitted = True on success
        - Must populate self._classes and self._feature_names
        - Must return a complete TrainingResult
        - Must NOT use any data from X_val/y_val during fitting
          (only for evaluation after training is complete)
        - May tune hyperparameters internally (e.g. Optuna), but only
          using X_train/y_train with cross-validation, not X_val/y_val

        Parameters
        ----------
        X_train / X_val : Feature DataFrames (named columns required)
        y_train / y_val : Target Series of string class labels

        Returns
        -------
        TrainingResult
        """
        ...

    @abstractmethod
    def predict_proba(self, X: pd.DataFrame) -> PredictionBatch:
        """
        Return calibrated class probabilities for each row in X.

        Contract
        --------
        - Must be thread-safe (acquire self._predict_lock)
        - Must raise RuntimeError if model is not fitted
        - Must align X columns to self._feature_names (missing → 0, extra → drop)
        - Must return a PredictionBatch with a DataFrame whose columns
          match self._classes exactly

        Parameters
        ----------
        X : Feature DataFrame. Columns may be a superset of training features.

        Returns
        -------
        PredictionBatch
        """
        ...

    @abstractmethod
    def calibrate(
        self,
        X_cal:  pd.DataFrame,
        y_cal:  pd.Series,
        method: str = CalibrationMethod.ISOTONIC,
    ) -> None:
        """
        Post-hoc probability calibration.

        Contract
        --------
        - Must be applied AFTER train()
        - Must use a HELD-OUT calibration set (not training data)
        - Must set self._calibrated = True
        - Calling predict_proba() after calibrate() returns calibrated probabilities

        Parameters
        ----------
        X_cal / y_cal : Calibration split (held-out from training)
        method        : CalibrationMethod constant
        """
        ...

    @abstractmethod
    def save(self, suffix: str = "") -> Path:
        """
        Persist model artifacts to self.model_dir.

        Contract
        --------
        - Must write a JSON metadata sidecar alongside the model file
          containing MODEL_SCHEMA_VERSION, sport, market, classes,
          feature_names, and training metrics
        - Must return the path of the primary model file
        - Must be atomic — write to a temp file, then rename

        Parameters
        ----------
        suffix : optional version string appended to filename

        Returns
        -------
        Path to the saved model file
        """
        ...

    @abstractmethod
    def load(self, path: Path) -> None:
        """
        Restore model from disk.

        Contract
        --------
        - Must read and validate the JSON metadata sidecar
        - Must raise ValueError if schema_version is incompatible
        - Must set self._fitted = True and restore self._classes,
          self._feature_names, self._calibrated
        - Must be callable on a freshly constructed (unfitted) instance

        Parameters
        ----------
        path : Path to the primary model file (not the sidecar)
        """
        ...

    @abstractmethod
    def get_feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        """
        Return a DataFrame of feature importances ranked descending.

        Contract
        --------
        - Must raise RuntimeError if model is not fitted
        - Must return a DataFrame with at minimum these columns:
            feature (str), importance (float), rank (int)
        - Prefer SHAP values if the subclass supports them
        - Fall back to native importances (tree.feature_importances_,
          LightGBM split gain, XGBoost gain, etc.)
        - For linear models: use |coefficient| × feature_std
        - For LSTM: use gradient × input (integrated gradients)

        Parameters
        ----------
        top_n : maximum number of features to return (sorted descending)

        Returns
        -------
        pd.DataFrame with columns [feature, importance, rank]
        """
        ...

    # ══════════════════════════════════════════════════════════════════════
    # Concrete methods — shared implementation, override if needed
    # ══════════════════════════════════════════════════════════════════════

    def backtest(
        self,
        X:              pd.DataFrame,
        y:              pd.Series,
        dates:          pd.Series,
        min_train_rows: int   = 500,
        n_folds:        int   = 5,
        edge_threshold: float = 0.03,
        retrain:        bool  = True,
    ) -> BacktestResult:
        """
        Walk-forward backtest with expanding training windows.

        Algorithm
        ---------
        Sort by date → split into n_folds equal temporal blocks:
          fold 0: train on block[0],       test on block[1]
          fold 1: train on block[0..1],    test on block[2]
          ...
          fold n-2: train on block[0..n-2], test on block[n-1]

        For each fold:
          1. (Re)train on expanding window
          2. Predict on held-out block
          3. Compute accuracy, log-loss, Brier, ROC-AUC
          4. Simulate unit-stake ROI: bet every prediction where
             model_prob > implied_prob + edge_threshold

        Parameters
        ----------
        X              : Full feature DataFrame
        y              : Full target Series
        dates          : Match date per row (used for temporal ordering)
        min_train_rows : Minimum rows in the first training fold; raise
                         ValueError if dataset is too small
        n_folds        : Number of temporal folds (default 5)
        edge_threshold : Minimum edge to simulate a bet (default 0.03)
        retrain        : If True, retrain on each fold. If False, use the
                         already-fitted model (faster but less realistic)

        Returns
        -------
        BacktestResult with per-fold metrics and aggregated statistics
        """
        from sklearn.metrics import (
            accuracy_score, log_loss, brier_score_loss, roc_auc_score
        )
        from sklearn.preprocessing import label_binarize

        if len(X) < min_train_rows + 50:
            raise ValueError(
                f"Dataset too small for backtest: {len(X)} rows "
                f"(need at least {min_train_rows + 50})"
            )

        # Sort by date
        order  = pd.to_datetime(dates, errors="coerce").argsort()
        X_s    = X.iloc[order].reset_index(drop=True)
        y_s    = y.iloc[order].reset_index(drop=True)

        # Fold boundaries
        n       = len(X_s)
        fold_sz = n // n_folds
        fold_metrics: list[dict] = []

        for fold in range(1, n_folds):
            train_end = fold * fold_sz
            test_end  = min((fold + 1) * fold_sz, n)
            if train_end < min_train_rows:
                logger.info("Backtest fold %d: skipping (only %d train rows)", fold, train_end)
                continue

            X_tr, y_tr = X_s.iloc[:train_end],         y_s.iloc[:train_end]
            X_te, y_te = X_s.iloc[train_end:test_end], y_s.iloc[train_end:test_end]

            if len(X_te) == 0:
                continue

            t0 = time.monotonic()
            if retrain:
                n_val = max(50, len(X_tr) // 8)
                X_tr_inner, X_val_inner = X_tr.iloc[:-n_val], X_tr.iloc[-n_val:]
                y_tr_inner, y_val_inner = y_tr.iloc[:-n_val], y_tr.iloc[-n_val:]
                try:
                    self.train(X_tr_inner, y_tr_inner, X_val_inner, y_val_inner)
                except Exception as exc:
                    logger.error("Backtest fold %d train failed: %s", fold, exc)
                    continue

            batch = self.predict_proba(X_te)
            proba = batch.probabilities.values
            preds = batch.predicted_class.values

            try:
                classes = self._classes
                le_map  = {c: i for i, c in enumerate(classes)}
                y_enc   = y_te.map(le_map).fillna(0).astype(int)
                y_bin   = label_binarize(y_enc, classes=list(range(len(classes))))

                acc  = accuracy_score(y_te, preds)
                ll   = log_loss(y_te, proba, labels=classes)
                bs   = float(np.mean([
                    brier_score_loss(y_bin[:, i], proba[:, i])
                    for i in range(len(classes))
                ]))
                auc  = roc_auc_score(y_bin, proba, multi_class="ovr", average="macro") if len(classes) > 2 else \
                       roc_auc_score(y_enc, proba[:, 1])
                roi  = self._simulate_roi(proba, y_te.values, classes, edge_threshold)
            except Exception as exc:
                logger.warning("Backtest fold %d metrics failed: %s", fold, exc)
                acc = ll = bs = auc = roi = float("nan")

            duration = time.monotonic() - t0
            fold_metrics.append({
                "fold":       fold,
                "n_train":    len(X_tr),
                "n_test":     len(X_te),
                "accuracy":   round(acc, 4),
                "log_loss":   round(ll,  4),
                "brier":      round(bs,  4),
                "roc_auc":    round(auc, 4),
                "roi":        round(roi, 4),
                "duration_s": round(duration, 2),
            })
            logger.info(
                "Backtest fold %d — acc=%.3f  ll=%.4f  roi=%+.2%%",
                fold, acc, ll, roi * 100,
            )

        # Aggregate
        metrics_df = pd.DataFrame(fold_metrics).dropna()
        result = BacktestResult(
            model_name=self.MODEL_NAME,
            sport=self.sport,
            market=self.market,
            n_folds=len(fold_metrics),
            fold_metrics=fold_metrics,
            mean_accuracy = float(metrics_df["accuracy"].mean()) if not metrics_df.empty else 0.0,
            mean_log_loss = float(metrics_df["log_loss"].mean()) if not metrics_df.empty else 0.0,
            mean_brier    = float(metrics_df["brier"].mean())    if not metrics_df.empty else 0.0,
            mean_roc_auc  = float(metrics_df["roc_auc"].mean())  if not metrics_df.empty else 0.0,
            std_accuracy  = float(metrics_df["accuracy"].std())  if not metrics_df.empty else 0.0,
            roi_simulated = float(metrics_df["roi"].mean())      if not metrics_df.empty else 0.0,
            sharpe_ratio  = self._sharpe(metrics_df["roi"].tolist()) if not metrics_df.empty else 0.0,
        )
        logger.info("Backtest complete:\n%s", result.summary())
        return result

    def predict(self, X: pd.DataFrame) -> pd.Series:
        """Convenience: return the most probable class for each row."""
        batch = self.predict_proba(X)
        return batch.predicted_class

    def classes(self) -> list[str]:
        """Return ordered class labels."""
        if not self._classes:
            raise RuntimeError("Model not fitted — call train() first")
        return list(self._classes)

    def is_fitted(self) -> bool:
        return self._fitted

    def is_calibrated(self) -> bool:
        return self._calibrated

    def feature_names(self) -> list[str]:
        return list(self._feature_names)

    def metadata(self) -> dict[str, Any]:
        """Return model metadata dict (schema version, classes, etc.)."""
        return {
            **self._metadata,
            "fitted":           self._fitted,
            "calibrated":       self._calibrated,
            "n_features":       len(self._feature_names),
            "classes":          self._classes,
        }

    # ── Column alignment ─────────────────────────────────────────

    def _align_features(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Align incoming DataFrame to self._feature_names.
        Missing columns → 0.0, extra columns → dropped.
        Column order guaranteed to match training.
        """
        if not self._feature_names:
            return X.fillna(0.0)
        missing = [c for c in self._feature_names if c not in X.columns]
        for col in missing:
            X = X.copy()
            X[col] = 0.0
        return X[self._feature_names].fillna(0.0)

    # ── Metric helpers ───────────────────────────────────────────

    @staticmethod
    def _compute_metrics(
        y_true:  pd.Series,
        proba:   np.ndarray,
        classes: list[str],
    ) -> dict[str, float]:
        """Compute standard classification metrics; return as dict."""
        from sklearn.metrics import accuracy_score, log_loss, brier_score_loss, roc_auc_score
        from sklearn.preprocessing import label_binarize

        le_map = {c: i for i, c in enumerate(classes)}
        y_enc  = y_true.map(le_map).fillna(0).astype(int)
        preds  = [classes[i] for i in proba.argmax(axis=1)]

        acc   = accuracy_score(y_true, preds)
        ll    = log_loss(y_true, proba, labels=classes)

        y_bin = label_binarize(y_enc, classes=list(range(len(classes))))
        bs    = float(np.mean([brier_score_loss(y_bin[:, i], proba[:, i])
                               for i in range(len(classes))]))
        try:
            if len(classes) > 2:
                auc = roc_auc_score(y_bin, proba, multi_class="ovr", average="macro")
            else:
                auc = roc_auc_score(y_enc, proba[:, 1])
        except Exception:
            auc = float("nan")

        train_ll = float("nan")  # caller must provide if available
        return {
            "train_log_loss":  train_ll,
            "val_log_loss":    round(ll,  4),
            "val_accuracy":    round(acc, 4),
            "val_brier_score": round(bs,  4),
            "val_roc_auc":     round(auc, 4),
        }

    @staticmethod
    def _simulate_roi(
        proba:           np.ndarray,
        y_true:          np.ndarray,
        classes:         list[str],
        edge_threshold:  float,
        implied_prob:    float = 0.33,   # fallback uniform implied prob
    ) -> float:
        """
        Simulate unit-stake betting ROI on predictions where:
        model_probability - implied_probability > edge_threshold
        Returns ROI as a decimal (0.10 = +10%).
        """
        bets = total_return = 0
        for i, cls in enumerate(proba.argmax(axis=1)):
            model_p = float(proba[i, cls])
            edge    = model_p - implied_prob
            if edge < edge_threshold:
                continue
            dec_odds = 1.0 / implied_prob   # back-of-envelope odds
            bets    += 1
            if y_true[i] == classes[cls]:
                total_return += dec_odds
        if bets == 0:
            return 0.0
        return (total_return - bets) / bets

    @staticmethod
    def _sharpe(roi_list: list[float], rfr: float = 0.0) -> float:
        """Risk-adjusted return: (mean_roi - rfr) / std_roi."""
        if len(roi_list) < 2:
            return 0.0
        arr  = np.array(roi_list, dtype=float)
        mean = arr.mean()
        std  = arr.std()
        return float((mean - rfr) / std) if std > 0 else 0.0

    # ── Metadata sidecar ────────────────────────────────────────

    def _write_metadata_sidecar(self, model_path: Path, extra: dict | None = None) -> Path:
        """Write a <model_path>.json metadata file alongside the model."""
        sidecar = model_path.with_suffix(".json")
        payload = {
            **self.metadata(),
            "feature_names":  self._feature_names,
            "saved_at":       datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            payload.update(extra)
        sidecar.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        return sidecar

    def _read_metadata_sidecar(self, model_path: Path) -> dict:
        """Read and validate the metadata sidecar for a model file."""
        sidecar = model_path.with_suffix(".json")
        if not sidecar.exists():
            logger.warning("No metadata sidecar found for %s", model_path.name)
            return {}
        with sidecar.open(encoding="utf-8") as f:
            meta = json.load(f)
        if meta.get("model_name") != self.MODEL_NAME:
            raise ValueError(
                f"Model name mismatch: file contains '{meta.get('model_name')}' "
                f"but this class is '{self.MODEL_NAME}'"
            )
        if meta.get("sport") != self.sport or meta.get("market") != self.market:
            raise ValueError(
                f"Sport/market mismatch: file is for {meta.get('sport')}/{meta.get('market')}, "
                f"but this instance is for {self.sport}/{self.market}"
            )
        return meta

    def _restore_from_metadata(self, meta: dict) -> None:
        """Restore classes, feature_names, calibration flag from sidecar."""
        self._classes       = meta.get("classes",       [])
        self._feature_names = meta.get("feature_names", [])
        self._calibrated    = meta.get("calibrated",    False)
        self._metadata.update({k: v for k, v in meta.items()
                               if k not in ("classes", "feature_names")})

    # ── String representation ────────────────────────────────────

    def __repr__(self) -> str:
        status = "fitted" if self._fitted else "unfitted"
        return (
            f"{self.MODEL_NAME}("
            f"sport={self.sport!r}, market={self.market!r}, "
            f"status={status}, calibrated={self._calibrated})"
        )

    def __str__(self) -> str:
        return self.__repr__()
