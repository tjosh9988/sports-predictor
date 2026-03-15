"""
ensemble.py — Level-2 Stacking Ensemble for Bet Hero ML pipeline.

Architecture
============

Level-1 (base models, already trained separately):
    RandomForest  ──┐
    XGBoost       ──┤
    LightGBM      ──┼─→  stacked probability matrix (N × n_classes × 5 models)
    NeuralNetwork ──┤                          ↓
    LSTM          ──┘         flatten → (N × n_classes*5 features)

Level-2 (meta-learner):
    XGBClassifier trained on stacked probabilities
    → calibrated with IsotonicRegression per class
    → final output: DataFrame(n_samples, n_classes) of calibrated probabilities

Key design decisions
====================
- Per-sport weights: each base model gets a weight proportional to its
  validation ROC-AUC on the current sport. Better models contribute more
  to the stacked feature matrix via soft weighting (see WeightedStackBuilder).
- Stacking uses OUT-OF-FOLD predictions to prevent leakage:
  base models are NOT re-trained here; their existing .predict_proba()
  on a held-out set provides the stacked features.
- Meta-learner Optuna tuning (20 trials, fast since input is small).
- IsotonicRegression calibration on a second held-out slice.
- Full BetHeroBaseModel compliance for save/load/backtest.
- Thread-safe predict_proba via inherited RLock.

Usage
=====
    from app.ml.models.ensemble import StackingEnsemble

    ensemble = StackingEnsemble(sport="football", market="match_result",
                                model_dir=Path("models/football/match_result"))
    ensemble.load_base_models()          # loads RF/XGB/LGB/NN/LSTM from disk
    result = ensemble.train(             # trains the meta-learner
        X_val, y_val,                    # held-out data from base model training
        X_cal, y_cal,                    # calibration split
    )
    batch = ensemble.predict_proba(X_new)
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import ClassVar

import joblib
import numpy as np
import pandas as pd
import optuna
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.preprocessing import LabelEncoder, label_binarize
from xgboost import XGBClassifier

from app.base_model import (
    BetHeroBaseModel,
    CalibrationMethod,
    PredictionBatch,
    TrainingResult,
)
from app.lstm_model import LSTMBetModel
from app.lightgbm_model import LightGBMBetModel
from app.neural_network_model import NeuralNetworkBetModel
from app.random_forest_model import RandomForestBetModel
from app.xgboost_model import XGBoostBetModel

optuna.logging.set_verbosity(optuna.logging.WARNING)
logger = logging.getLogger(__name__)

# ─────────────────────────── Constants ────────────────────────────────────

BASE_MODEL_NAMES: list[str] = [
    "RandomForestBetModel",
    "XGBoostBetModel",
    "LightGBMBetModel",
    "NeuralNetworkBetModel",
    "LSTMBetModel",
]

META_OPTUNA_TRIALS = 20     # fast — stacked feature space is small


# ─────────────────────────── Per-sport weight registry ────────────────────

class SportWeightRegistry:
    """
    Stores and retrieves per-sport, per-model weights derived from
    validation ROC-AUC scores collected during base model training.

    Weights are normalised per sport so they sum to 1 across models.
    Models with zero or unavailable weights fall back to uniform weights.

    Persistence: written as JSON alongside the ensemble model file.
    """

    def __init__(self):
        # { sport: { model_name: float } }
        self._store: dict[str, dict[str, float]] = {}

    def update(self, sport: str, model_name: str, roc_auc: float) -> None:
        """Record a model's validation ROC-AUC for a sport."""
        self._store.setdefault(sport, {})[model_name] = max(0.0, float(roc_auc))

    def weights_for_sport(
        self, sport: str, model_names: list[str]
    ) -> dict[str, float]:
        """
        Return normalised weights for each model.
        Weight = softmax(roc_auc²) — squaring amplifies the gap between models.
        Falls back to uniform if no data available.
        """
        raw = self._store.get(sport, {})
        scores = np.array([raw.get(m, 0.5) for m in model_names], dtype=np.float64)
        # Softmax on squared scores to amplify differences
        x = scores ** 2
        e = np.exp(x - x.max())
        weights = e / e.sum()
        return {m: float(w) for m, w in zip(model_names, weights)}

    def to_dict(self) -> dict:
        return dict(self._store)

    def from_dict(self, d: dict) -> None:
        self._store = d


# ─────────────────────────── Stacked Feature Builder ──────────────────────

class WeightedStackBuilder:
    """
    Converts base-model probability outputs into a weighted stacked feature matrix.

    For each base model m with weight w_m:
      - raw stacked features : model_proba  → shape (N, n_classes)
      - weighted features    : model_proba * w_m

    The final stacked matrix concatenates both weighted and raw columns
    so the meta-learner can learn to de-weight poor models while still
    having access to the full unweighted signal:

      stacked  = [w1*proba1, ..., w5*proba5,     ← weighted
                   proba1, ..., proba5]           ← raw (unweighted)

    Shape: (N, n_classes * n_models * 2)
    """

    def __init__(self, classes: list[str], model_names: list[str]):
        self.classes     = classes
        self.model_names = model_names

    def build(
        self,
        proba_dict: dict[str, np.ndarray],    # {model_name: (N, n_classes)}
        weights:    dict[str, float],          # {model_name: float 0-1}
    ) -> np.ndarray:
        """
        Parameters
        ----------
        proba_dict : probability matrices from each base model
        weights    : per-model weights (should sum to 1)

        Returns
        -------
        np.ndarray of shape (N, n_classes * n_models * 2)
        """
        weighted_cols: list[np.ndarray] = []
        raw_cols:      list[np.ndarray] = []

        for name in self.model_names:
            proba = proba_dict.get(name)
            if proba is None:
                # Missing model — fill with uniform probability
                n = next(iter(proba_dict.values())).shape[0]
                proba = np.full((n, len(self.classes)), 1.0 / len(self.classes))
            w = weights.get(name, 1.0 / len(self.model_names))
            weighted_cols.append(proba * w)
            raw_cols.append(proba)

        return np.hstack(weighted_cols + raw_cols)

    def feature_names(self) -> list[str]:
        names: list[str] = []
        for suffix in ("w", "raw"):
            for mdl in self.model_names:
                short = mdl.replace("BetModel", "")
                for cls in self.classes:
                    names.append(f"{suffix}_{short}_{cls}")
        return names


# ─────────────────────────── Stacking Ensemble ────────────────────────────

class StackingEnsemble(BetHeroBaseModel):
    """
    Level-2 stacking ensemble.

    Trains an XGBoost meta-learner on stacked probability outputs from
    base models (RF, XGB, LGB, NN, LSTM). Calibrated with IsotonicRegression.
    Per-sport weights bias the stacked input toward stronger base models.
    """

    MODEL_NAME: ClassVar[str]                  = "StackingEnsemble"
    SUPPORTS_FEATURE_IMPORTANCE: ClassVar[bool] = True

    def __init__(
        self,
        sport:      str,
        market:     str,
        model_dir:  Path | None = None,
        n_meta_trials: int = META_OPTUNA_TRIALS,
    ):
        super().__init__(sport, market, model_dir)
        self.n_meta_trials  = n_meta_trials

        # Base models (loaded from disk)
        self._base_models:  dict[str, BetHeroBaseModel] = {}
        self._base_names:   list[str]                   = []

        # Meta-learner
        self._meta:         XGBClassifier | None         = None
        self._le:           LabelEncoder                 = LabelEncoder()
        self._cals:         list[IsotonicRegression]     = []

        # Weighting
        self._weight_reg:   SportWeightRegistry          = SportWeightRegistry()
        self._weights:      dict[str, float]             = {}

        # Stack builder
        self._stack_builder: WeightedStackBuilder | None = None

    # ── Load base models from disk ─────────────────────────────────────────

    def load_base_models(self, model_dir: Path | None = None) -> dict[str, bool]:
        """
        Discover and load all available base model files from disk.

        Expected file naming convention (from training_pipeline.py):
            <ModelName>_<sport>_<market>.joblib  or  .pt

        Returns {model_name: loaded_ok}
        """
        search_dir = model_dir or self.model_dir
        loaded: dict[str, bool] = {}

        candidates: list[tuple[str, type[BetHeroBaseModel], list[str]]] = [
            ("RandomForestBetModel",  RandomForestBetModel,  [".joblib"]),
            ("XGBoostBetModel",       XGBoostBetModel,       [".joblib"]),
            ("LightGBMBetModel",      LightGBMBetModel,      [".joblib"]),
            ("NeuralNetworkBetModel", NeuralNetworkBetModel, [".pt"]),
            ("LSTMBetModel",          LSTMBetModel,          [".pt"]),
        ]

        for model_name, cls, exts in candidates:
            ok = False
            for ext in exts:
                pattern = f"{model_name}_{self.sport}_{self.market}{ext}"
                path    = search_dir / pattern
                if not path.exists():
                    # Try subdirectory layout
                    path = search_dir.parent / model_name / pattern
                if path.exists():
                    try:
                        instance = cls(sport=self.sport, market=self.market,
                                       model_dir=search_dir)
                        instance.load(path)
                        self._base_models[model_name] = instance
                        logger.info("[Ensemble] Loaded base model: %s", model_name)
                        ok = True
                        break
                    except Exception as exc:
                        logger.warning("[Ensemble] Failed to load %s: %s", model_name, exc)
            loaded[model_name] = ok

        self._base_names = list(self._base_models.keys())
        n_loaded = sum(loaded.values())
        logger.info("[Ensemble] %d/%d base models loaded for %s/%s",
                    n_loaded, len(candidates), self.sport, self.market)
        return loaded

    def add_base_model(self, name: str, model: BetHeroBaseModel) -> None:
        """Manually register an already-trained base model."""
        self._base_models[name] = model
        if name not in self._base_names:
            self._base_names.append(name)

    # ── Collect base model probabilities ───────────────────────────────────

    def _collect_probas(self, X: pd.DataFrame) -> dict[str, np.ndarray]:
        """
        Run each base model's predict_proba and return {name: proba_matrix}.
        Missing or errored models return uniform probability.
        """
        n_cls  = len(self._classes)
        result: dict[str, np.ndarray] = {}
        for name, mdl in self._base_models.items():
            try:
                batch = mdl.predict_proba(X)
                # Reorder columns to match ensemble class order
                proba = batch.probabilities.reindex(
                    columns=self._classes, fill_value=1.0 / n_cls
                ).values
            except Exception as exc:
                logger.warning("[Ensemble] Base model %s predict failed: %s", name, exc)
                proba = np.full((len(X), n_cls), 1.0 / n_cls)
            result[name] = proba
        return result

    def _build_stack(
        self,
        X: pd.DataFrame,
        weights: dict[str, float] | None = None,
    ) -> np.ndarray:
        """Produce the stacked feature matrix for X."""
        w       = weights or self._weights or self._uniform_weights()
        probas  = self._collect_probas(X)
        return self._stack_builder.build(probas, w)

    def _uniform_weights(self) -> dict[str, float]:
        n = max(len(self._base_names), 1)
        return {m: 1.0 / n for m in self._base_names}

    # ── Compute per-sport weights from validation performance ──────────────

    def compute_sport_weights(
        self,
        X_val:  pd.DataFrame,
        y_val:  pd.Series,
    ) -> dict[str, float]:
        """
        Evaluate each base model on X_val/y_val and derive weights from
        their per-sport ROC-AUC scores.

        Call this BEFORE train() to use sport-specific weights.
        """
        le_m   = {c: i for i, c in enumerate(self._classes)}
        y_enc  = y_val.map(le_m).fillna(0).astype(int).values
        n_cls  = len(self._classes)
        y_bin  = label_binarize(y_enc, classes=list(range(n_cls)))

        for name, mdl in self._base_models.items():
            try:
                batch = mdl.predict_proba(X_val)
                proba = batch.probabilities.reindex(
                    columns=self._classes, fill_value=1.0 / n_cls
                ).values
                if n_cls > 2:
                    auc = roc_auc_score(y_bin, proba, multi_class="ovr", average="macro")
                else:
                    auc = roc_auc_score(y_enc, proba[:, 1])
                self._weight_reg.update(self.sport, name, auc)
                logger.info("[Ensemble] %s  val ROC-AUC=%.4f → weight updated", name, auc)
            except Exception as exc:
                logger.warning("[Ensemble] Weight eval failed for %s: %s", name, exc)
                self._weight_reg.update(self.sport, name, 0.5)

        self._weights = self._weight_reg.weights_for_sport(self.sport, self._base_names)
        logger.info("[Ensemble] Sport weights for %s: %s",
                    self.sport,
                    "  ".join(f"{k.replace('BetModel','')[:6]}={v:.3f}"
                              for k, v in self._weights.items()))
        return self._weights

    # ── train ──────────────────────────────────────────────────────────────

    def train(
        self,
        X_train:  pd.DataFrame,
        y_train:  pd.Series,
        X_val:    pd.DataFrame,
        y_val:    pd.Series,
        **kwargs,
    ) -> TrainingResult:
        """
        Train the XGBoost meta-learner on stacked base model outputs.

        Parameters
        ----------
        X_train / y_train : Held-out data from base model training phase
                            (this is the base models' original validation set)
        X_val   / y_val   : A second held-out slice for meta-learner validation
                            (must be chronologically AFTER X_train)

        NOTE: X_train here should NOT overlap with any data used to train the
              base models. Use the cal / test slices from training_pipeline.py.
        """
        if not self._base_models:
            raise RuntimeError("No base models loaded — call load_base_models() first")

        t0 = time.monotonic()
        self._classes       = sorted(y_train.unique().tolist())
        self._feature_names = X_train.columns.tolist()
        n_cls               = len(self._classes)
        multiclass          = n_cls > 2

        # ── Initialise stack builder ─────────────────────────────
        self._stack_builder = WeightedStackBuilder(self._classes, self._base_names)

        # ── Compute sport weights from train split ────────────────
        if not self._weights:
            self.compute_sport_weights(X_train, y_train)

        # ── Build stacked matrices ────────────────────────────────
        logger.info("[Ensemble] Building stacked features …")
        Z_train = self._build_stack(X_train)
        Z_val   = self._build_stack(X_val)

        y_tr_enc = self._le.fit_transform(y_train)
        y_va_enc = self._le.transform(y_val)

        meta_obj    = "multi:softprob" if multiclass else "binary:logistic"
        meta_metric = "mlogloss"       if multiclass else "logloss"
        base_kw: dict = {
            "objective":          meta_obj,
            "eval_metric":        meta_metric,
            "use_label_encoder":  False,
            "verbosity":          0,
            "random_state":       42,
        }
        if multiclass:
            base_kw["num_class"] = n_cls

        # ── Optuna meta-learner tuning ────────────────────────────
        def _obj(trial: optuna.Trial) -> float:
            params = base_kw | {
                "n_estimators":     trial.suggest_int("n_estimators", 50, 500, log=True),
                "max_depth":        trial.suggest_int("max_depth", 2, 8),
                "learning_rate":    trial.suggest_float("lr", 0.01, 0.3, log=True),
                "subsample":        trial.suggest_float("subsample", 0.6, 1.0),
                "colsample_bytree": trial.suggest_float("colsample_bytree", 0.6, 1.0),
                "reg_alpha":        trial.suggest_float("reg_alpha",  1e-8, 5.0, log=True),
                "reg_lambda":       trial.suggest_float("reg_lambda", 1e-8, 5.0, log=True),
                "min_child_weight": trial.suggest_int("min_child_weight", 1, 10),
                "gamma":            trial.suggest_float("gamma", 0.0, 3.0),
            }
            mdl = XGBClassifier(**params)
            mdl.fit(Z_train, y_tr_enc,
                    eval_set=[(Z_val, y_va_enc)], verbose=False)
            return log_loss(y_va_enc, mdl.predict_proba(Z_val))

        study = optuna.create_study(
            direction="minimize",
            sampler=optuna.samplers.TPESampler(seed=42, n_startup_trials=5),
        )
        study.optimize(_obj, n_trials=self.n_meta_trials, show_progress_bar=False)
        logger.info("[Ensemble] Meta best val log-loss: %.4f", study.best_value)

        best = base_kw | study.best_params
        self._meta = XGBClassifier(**best)
        self._meta.fit(Z_train, y_tr_enc,
                       eval_set=[(Z_val, y_va_enc)], verbose=False)

        val_proba = self._meta.predict_proba(Z_val)
        train_ll  = log_loss(y_tr_enc, self._meta.predict_proba(Z_train))
        metrics   = self._compute_metrics(y_val, val_proba, self._classes)
        self._fitted = True

        result = TrainingResult(
            model_name=self.MODEL_NAME, sport=self.sport, market=self.market,
            n_train=len(Z_train), n_val=len(Z_val),
            n_features=Z_train.shape[1],
            train_log_loss=round(train_ll, 4),
            best_params=best,
            duration_s=round(time.monotonic() - t0, 1),
            **{k: v for k, v in metrics.items() if k != "train_log_loss"},
        )
        result.train_log_loss = round(train_ll, 4)
        self._training_result = result
        logger.info("[Ensemble/%s/%s]\n%s", self.sport, self.market, result.summary())
        return result

    # ── calibrate (Isotonic per class) ────────────────────────────────────

    def calibrate(
        self,
        X_cal: pd.DataFrame,
        y_cal: pd.Series,
        method: str = CalibrationMethod.ISOTONIC,
    ) -> None:
        if not self._fitted:
            raise RuntimeError("Call train() first")
        Z_cal  = self._build_stack(X_cal)
        raw    = self._meta.predict_proba(Z_cal)
        n_cls  = len(self._classes)
        le_m   = {c: i for i, c in enumerate(self._classes)}
        y_enc  = y_cal.map(le_m).fillna(0).astype(int).values
        y_ohe  = np.eye(n_cls)[y_enc]
        self._cals = []
        for i in range(n_cls):
            iso = IsotonicRegression(out_of_bounds="clip")
            iso.fit(raw[:, i], y_ohe[:, i])
            self._cals.append(iso)
        self._calibrated = True
        logger.info("[Ensemble] Calibrated on %d samples", len(X_cal))

    def _apply_cal(self, p: np.ndarray) -> np.ndarray:
        if not self._calibrated or not self._cals:
            return p
        cols = np.column_stack([c.predict(p[:, i]) for i, c in enumerate(self._cals)])
        s    = cols.sum(axis=1, keepdims=True)
        return cols / np.where(s == 0, 1.0, s)

    # ── predict_proba ──────────────────────────────────────────────────────

    def predict_proba(self, X: pd.DataFrame) -> PredictionBatch:
        """
        Full level-2 prediction pipeline:
          1. Collect base model probabilities
          2. Build weighted stacked matrix
          3. XGBoost meta-learner forward pass
          4. Isotonic calibration
          5. Return PredictionBatch
        """
        if not self._fitted:
            raise RuntimeError("Ensemble not trained — call train() first")
        with self._predict_lock:
            Z     = self._build_stack(X)
            raw   = self._meta.predict_proba(Z)
            p_cal = self._apply_cal(raw)
            df    = pd.DataFrame(p_cal, columns=self._classes)
            return PredictionBatch(
                probabilities=df,
                predicted_class=df.idxmax(axis=1),
                confidence=df.max(axis=1),
                model_name=self.MODEL_NAME,
                sport=self.sport,
                market=self.market,
            )

    # ── save / load ────────────────────────────────────────────────────────

    def save(self, suffix: str = "") -> Path:
        path = self.model_dir / f"{self.MODEL_NAME}_{self.sport}_{self.market}{suffix}.joblib"
        tmp  = path.with_suffix(".tmp")
        joblib.dump(
            {
                "meta":         self._meta,
                "le":           self._le,
                "cals":         self._cals,
                "classes":      self._classes,
                "feature_names": self._feature_names,
                "base_names":   self._base_names,
                "weights":      self._weights,
                "weight_reg":   self._weight_reg.to_dict(),
                "calibrated":   self._calibrated,
            },
            tmp,
        )
        tmp.rename(path)
        self._write_metadata_sidecar(path, extra={
            "base_models": self._base_names,
            "sport_weights": self._weights,
        })
        logger.info("[Ensemble] Saved → %s", path)
        return path

    def load(self, path: Path) -> None:
        meta = self._read_metadata_sidecar(path)
        obj  = joblib.load(path)
        self._meta          = obj["meta"]
        self._le            = obj["le"]
        self._cals          = obj.get("cals", [])
        self._classes       = obj["classes"]
        self._feature_names = obj["feature_names"]
        self._base_names    = obj.get("base_names", [])
        self._weights       = obj.get("weights", {})
        self._calibrated    = obj.get("calibrated", False)
        self._weight_reg    = SportWeightRegistry()
        self._weight_reg.from_dict(obj.get("weight_reg", {}))
        self._stack_builder = WeightedStackBuilder(self._classes, self._base_names)
        self._fitted        = True
        logger.info("[Ensemble] Loaded from %s", path)

    # ── feature importance ─────────────────────────────────────────────────

    def get_feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        if not self._fitted or self._meta is None:
            raise RuntimeError("Ensemble not trained")
        if self._stack_builder is None:
            raise RuntimeError("Stack builder not initialised — train() first")

        feat_names = self._stack_builder.feature_names()
        scores_    = self._meta.get_booster().get_score(importance_type="gain")
        # XGB names features as f0, f1, ... map back to our names
        importance = np.array([
            scores_.get(f"f{i}", 0.0) for i in range(len(feat_names))
        ])

        df = (
            pd.DataFrame({"feature": feat_names, "importance": importance})
            .sort_values("importance", ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )
        df["rank"] = df.index + 1
        return df

    # ── Ensemble-specific: weight report ──────────────────────────────────

    def weight_report(self) -> pd.DataFrame:
        """Return a DataFrame summarising the per-sport per-model weights."""
        rows = []
        for sport, model_weights in self._weight_reg.to_dict().items():
            for model, score in model_weights.items():
                normalised = self._weights.get(model, 0.0) if sport == self.sport else 0.0
                rows.append({
                    "sport":      sport,
                    "model":      model.replace("BetModel", ""),
                    "roc_auc":    round(score, 4),
                    "weight":     round(normalised, 4),
                })
        return pd.DataFrame(rows).sort_values(["sport", "weight"], ascending=[True, False])

    # ── Convenience: build + train in one call ─────────────────────────────

    @classmethod
    def from_trained_pipeline(
        cls,
        sport:      str,
        market:     str,
        X_meta_train: pd.DataFrame,
        y_meta_train: pd.Series,
        X_meta_val:   pd.DataFrame,
        y_meta_val:   pd.Series,
        X_cal:        pd.DataFrame,
        y_cal:        pd.Series,
        model_dir:    Path,
        n_trials:     int = META_OPTUNA_TRIALS,
    ) -> "StackingEnsemble":
        """
        Factory method: loads base models, computes weights, trains meta-learner,
        calibrates, and saves — all in one call.

        Parameters
        ----------
        X_meta_train / y_meta_train : data the meta-learner trains on
                                      (must be OOF predictions or a held-out slice)
        X_meta_val   / y_meta_val   : meta-learner validation
        X_cal        / y_cal        : calibration split (after X_meta_val)
        model_dir                   : directory holding base model files
        """
        ensemble = cls(sport=sport, market=market,
                       model_dir=model_dir, n_meta_trials=n_trials)
        ensemble.load_base_models(model_dir)

        if not ensemble._base_models:
            raise RuntimeError(
                f"No base models found in {model_dir} for {sport}/{market}. "
                "Run the training pipeline first."
            )

        # Weight computation uses the meta train split
        ensemble.compute_sport_weights(X_meta_train, y_meta_train)

        result = ensemble.train(X_meta_train, y_meta_train, X_meta_val, y_meta_val)
        ensemble.calibrate(X_cal, y_cal)
        ensemble.save()
        return ensemble
