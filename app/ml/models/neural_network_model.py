"""
neural_network_model.py — 4-hidden-layer Feedforward NN (PyTorch) for Bet Hero.

Architecture
------------
Input(F) → BN → Block1(512) → Block2(256) → Block3(128) → Block4(64)
         → residual(input→64) → Linear(n_classes)

Each Block = Linear → BN → GELU → Dropout

Extras
------
- Label-smoothing cross-entropy loss (ε=0.05)
- CosineAnnealingWarmRestarts + gradient clipping
- Early stopping (patience=20, check every 5 epochs)
- Temperature-scaling calibration via L-BFGS
- Gradient × input sensitivity for feature importance
- StandardScaler preprocessing
- Atomic .pt save + JSON metadata sidecar
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
from sklearn.metrics import log_loss
from sklearn.preprocessing import LabelEncoder, StandardScaler

from app.base_model import (
    BetHeroBaseModel,
    CalibrationMethod,
    PredictionBatch,
    TrainingResult,
)

logger = logging.getLogger(__name__)
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"


# ──────────────────────────── Network Building Blocks ────────────────────────

class _Block(nn.Sequential):
    def __init__(self, d_in: int, d_out: int, dropout: float):
        super().__init__(
            nn.Linear(d_in, d_out),
            nn.BatchNorm1d(d_out),
            nn.GELU(),
            nn.Dropout(dropout),
        )


class _FFNet(nn.Module):
    """4-hidden-layer feedforward net with BatchNorm, GELU, residual skip."""

    def __init__(self, in_dim: int, hidden: list[int], n_cls: int, dropout: float):
        super().__init__()
        assert len(hidden) == 4
        self.input_bn = nn.BatchNorm1d(in_dim)
        self.b1 = _Block(in_dim,     hidden[0], dropout)
        self.b2 = _Block(hidden[0],  hidden[1], dropout)
        self.b3 = _Block(hidden[1],  hidden[2], dropout)
        self.b4 = _Block(hidden[2],  hidden[3], dropout)
        self.skip = nn.Linear(in_dim, hidden[3], bias=False)
        self.head = nn.Linear(hidden[3], n_cls)
        self._init()

    def _init(self):
        for m in self.modules():
            if isinstance(m, nn.Linear):
                nn.init.kaiming_normal_(m.weight, nonlinearity="relu")
                if m.bias is not None:
                    nn.init.zeros_(m.bias)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        xn = self.input_bn(x)
        h  = self.b4(self.b3(self.b2(self.b1(xn))))
        return self.head(h + self.skip(xn))  # residual


class _TempScaler(nn.Module):
    def __init__(self):
        super().__init__()
        self.T = nn.Parameter(torch.tensor(1.5))

    def forward(self, logits: torch.Tensor) -> torch.Tensor:
        return logits / self.T.clamp(min=0.05)


class _SmoothCE(nn.Module):
    def __init__(self, n_cls: int, eps: float = 0.05):
        super().__init__()
        self.n, self.eps = n_cls, eps

    def forward(self, logits: torch.Tensor, y: torch.Tensor) -> torch.Tensor:
        with torch.no_grad():
            s = torch.full_like(logits, self.eps / (self.n - 1))
            s.scatter_(1, y.unsqueeze(1), 1.0 - self.eps)
        return -(s * F.log_softmax(logits, 1)).sum(1).mean()


# ──────────────────────────── Model Class ────────────────────────────────────

class NeuralNetworkBetModel(BetHeroBaseModel):
    """4-hidden-layer feedforward NN (512→256→128→64) with temperature scaling."""

    MODEL_NAME  = "NeuralNetworkBetModel"
    SUPPORTS_FEATURE_IMPORTANCE = True
    DEFAULT_HIDDEN = [512, 256, 128, 64]

    def __init__(
        self,
        sport:       str,
        market:      str,
        hidden_dims: list[int] | None = None,
        dropout:     float = 0.30,
        epochs:      int   = 200,
        lr:          float = 3e-4,
        batch_size:  int   = 512,
        patience:    int   = 20,
        model_dir:   Path | None = None,
    ):
        super().__init__(sport, market, model_dir)
        self.hidden_dims = hidden_dims or self.DEFAULT_HIDDEN
        self.dropout     = dropout
        self.epochs      = epochs
        self.lr          = lr
        self.batch_size  = batch_size
        self.patience    = patience
        self._net:  _FFNet | None      = None
        self._ts:   _TempScaler | None = None
        self._sc:   StandardScaler     = StandardScaler()
        self._le:   LabelEncoder       = LabelEncoder()
        self._n_cls: int               = 0

    # ── helpers ──────────────────────────────────────────────────────────────

    def _to_tensor(self, X: pd.DataFrame, fit_scale: bool = False) -> torch.Tensor:
        arr = X.values.astype(np.float32)
        arr = self._sc.fit_transform(arr) if fit_scale else self._sc.transform(arr)
        return torch.tensor(arr, dtype=torch.float32, device=DEVICE)

    def _proba_np(self, X_t: torch.Tensor) -> np.ndarray:
        self._net.eval()
        with torch.no_grad():
            logits = self._net(X_t)
            if self._calibrated and self._ts:
                logits = self._ts(logits)
            return torch.softmax(logits, 1).cpu().numpy()

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
        self._n_cls         = len(self._classes)

        Xtr = self._to_tensor(X_train, fit_scale=True)
        Xva = self._to_tensor(X_val)
        ytr = torch.tensor(self._le.fit_transform(y_train).astype(np.int64),
                           dtype=torch.long, device=DEVICE)
        yva = torch.tensor(self._le.transform(y_val).astype(np.int64),
                           dtype=torch.long, device=DEVICE)

        self._net = _FFNet(len(self._feature_names), self.hidden_dims,
                           self._n_cls, self.dropout).to(DEVICE)
        crit = _SmoothCE(self._n_cls).to(DEVICE)
        opt  = torch.optim.AdamW(self._net.parameters(), lr=self.lr, weight_decay=1e-4)
        sch  = torch.optim.lr_scheduler.CosineAnnealingWarmRestarts(opt, T_0=40, T_mult=2)

        best_vloss, best_state, pctr = float("inf"), None, 0

        for ep in range(self.epochs):
            self._net.train()
            perm = torch.randperm(len(Xtr), device=DEVICE)
            n_b  = max(1, len(Xtr) // self.batch_size)
            for b in range(n_b):
                idx = perm[b * self.batch_size:(b + 1) * self.batch_size]
                opt.zero_grad()
                loss = crit(self._net(Xtr[idx]), ytr[idx])
                loss.backward()
                nn.utils.clip_grad_norm_(self._net.parameters(), 1.0)
                opt.step()
            sch.step()

            if ep % 5 == 0:
                self._net.eval()
                with torch.no_grad():
                    vl = F.cross_entropy(self._net(Xva), yva).item()
                if vl < best_vloss:
                    best_vloss = vl
                    best_state = {k: v.cpu().clone() for k, v in self._net.state_dict().items()}
                    pctr = 0
                else:
                    pctr += 1
                    if pctr >= self.patience:
                        logger.info("[NN] Early-stop at epoch %d (val_loss=%.4f)", ep, vl)
                        break

        if best_state:
            self._net.load_state_dict(best_state)

        self._fitted  = True
        val_proba     = self._proba_np(Xva)
        train_ll      = log_loss(self._le.transform(y_train), self._proba_np(Xtr))
        metrics       = self._compute_metrics(y_val, val_proba, self._classes)

        result = TrainingResult(
            model_name=self.MODEL_NAME, sport=self.sport, market=self.market,
            n_train=len(X_train), n_val=len(X_val),
            n_features=len(self._feature_names),
            train_log_loss=round(train_ll, 4),
            best_params={"hidden": self.hidden_dims, "dropout": self.dropout,
                         "lr": self.lr, "epochs": self.epochs},
            duration_s=round(time.monotonic() - t0, 1),
            **{k: v for k, v in metrics.items() if k != "train_log_loss"},
        )
        result.train_log_loss = round(train_ll, 4)
        self._training_result = result
        logger.info("[NN/%s/%s]\n%s", self.sport, self.market, result.summary())
        return result

    # ── calibrate (temperature scaling) ──────────────────────────────────────

    def calibrate(
        self,
        X_cal: pd.DataFrame,
        y_cal: pd.Series,
        method: str = CalibrationMethod.TEMPERATURE,
    ) -> None:
        if not self._fitted:
            raise RuntimeError("Call train() first")
        X_t  = self._to_tensor(self._align_features(X_cal))
        le_m = {c: i for i, c in enumerate(self._classes)}
        y_t  = torch.tensor(y_cal.map(le_m).fillna(0).astype(np.int64).values,
                            dtype=torch.long, device=DEVICE)
        self._net.eval()
        with torch.no_grad():
            logits = self._net(X_t)

        self._ts  = _TempScaler().to(DEVICE)
        opt = torch.optim.LBFGS([self._ts.T], lr=0.05, max_iter=300)

        def _step():
            opt.zero_grad()
            loss = F.cross_entropy(self._ts(logits), y_t)
            loss.backward()
            return loss

        opt.step(_step)
        self._calibrated = True
        logger.info("[NN] Temperature=%.4f after calibration", float(self._ts.T.item()))

    # ── predict_proba ────────────────────────────────────────────────────────

    def predict_proba(self, X: pd.DataFrame) -> PredictionBatch:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        with self._predict_lock:
            X_t   = self._to_tensor(self._align_features(X))
            proba = self._proba_np(X_t)
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
        path = self.model_dir / f"{self.MODEL_NAME}_{self.sport}_{self.market}{suffix}.pt"
        tmp  = path.with_suffix(".tmp")
        torch.save({
            "net":    self._net.state_dict() if self._net else {},
            "ts":     self._ts.state_dict()  if self._ts  else {},
            "sc":     self._sc,
            "le":     self._le,
            "hidden": self.hidden_dims,
            "drop":   self.dropout,
            "n_cls":  self._n_cls,
            "clss":   self._classes,
            "feats":  self._feature_names,
            "cal":    self._calibrated,
        }, tmp)
        tmp.rename(path)
        self._write_metadata_sidecar(path)
        logger.info("[NN] Saved → %s", path)
        return path

    def load(self, path: Path) -> None:
        self._read_metadata_sidecar(path)
        c = torch.load(path, map_location="cpu")
        self.hidden_dims    = c["hidden"]
        self.dropout        = c["drop"]
        self._n_cls         = c["n_cls"]
        self._classes       = c["clss"]
        self._feature_names = c["feats"]
        self._sc, self._le  = c["sc"], c["le"]
        self._calibrated    = c.get("cal", False)
        self._net = _FFNet(len(self._feature_names), self.hidden_dims,
                           self._n_cls, self.dropout).to(DEVICE)
        self._net.load_state_dict(c["net"])
        self._net.eval()
        if self._calibrated and c.get("ts"):
            self._ts = _TempScaler().to(DEVICE)
            self._ts.load_state_dict(c["ts"])
        self._fitted = True

    # ── feature importance ───────────────────────────────────────────────────

    def get_feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        x = torch.zeros(1, len(self._feature_names),
                        requires_grad=True, device=DEVICE)
        self._net.eval()
        self._net(x).sum().backward()
        imp = x.grad.abs().squeeze().detach().cpu().numpy()
        df  = (
            pd.DataFrame({"feature": self._feature_names, "importance": imp})
            .sort_values("importance", ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )
        df["rank"] = df.index + 1
        return df
