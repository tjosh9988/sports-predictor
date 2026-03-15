"""
lstm_model.py — Bidirectional Attention-LSTM (PyTorch) for Bet Hero.

Architecture
------------
Sequential team-form input (batch, seq_len=10, n_features)
→ optional InputNorm (LayerNorm per timestep)
→ BiLSTM (2 layers, hidden=128, bidirectional=True)
→ Attention (learned timestep weighting)
→ LayerNorm + Dropout on pooled vector
→ FC head: Linear(256) → ReLU → Dropout → Linear(n_classes)

Key design decisions
--------------------
- build_sequences() static method converts flat history DataFrame into
  (n_matches, seq_len, n_features) sequences, zero-padding short histories
- Falls back gracefully to seq_len=1 when no sequence data is available
- OneCycleLR scheduler for fast convergence
- Per-class IsotonicRegression calibration
- Gradient-sensitivity feature importance
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
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import log_loss
from sklearn.preprocessing import LabelEncoder, StandardScaler

from app.base_model import (
    BetHeroBaseModel,
    CalibrationMethod,
    PredictionBatch,
    TrainingResult,
)

logger = logging.getLogger(__name__)
DEVICE  = "cuda" if torch.cuda.is_available() else "cpu"
SEQ_LEN = 10


# ──────────────────────────── Network ────────────────────────────────────────

class _AttnBiLSTM(nn.Module):
    def __init__(self, in_dim: int, hidden: int, n_layers: int,
                 n_cls: int, dropout: float):
        super().__init__()
        self.norm  = nn.LayerNorm(in_dim)
        self.lstm  = nn.LSTM(in_dim, hidden, n_layers,
                             batch_first=True, bidirectional=True,
                             dropout=dropout if n_layers > 1 else 0.0)
        self.attn  = nn.Linear(hidden * 2, 1)
        self.post_norm = nn.LayerNorm(hidden * 2)
        self.drop  = nn.Dropout(dropout)
        self.head  = nn.Sequential(
            nn.Linear(hidden * 2, hidden),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden, n_cls),
        )
        self._init()

    def _init(self):
        for name, p in self.lstm.named_parameters():
            if "weight_ih" in name:
                nn.init.xavier_uniform_(p)
            elif "weight_hh" in name:
                nn.init.orthogonal_(p)
            elif "bias" in name:
                nn.init.zeros_(p)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (B, T, F)
        x = self.norm(x)
        out, _  = self.lstm(x)              # (B, T, 2H)
        scores  = self.attn(out)            # (B, T, 1)
        weights = torch.softmax(scores, 1)  # (B, T, 1)
        pooled  = (out * weights).sum(1)    # (B, 2H)  — attention-weighted mean
        pooled  = self.post_norm(self.drop(pooled))
        return self.head(pooled)


# ──────────────────────────── Model Class ────────────────────────────────────

class LSTMBetModel(BetHeroBaseModel):
    """
    Bidirectional attention-LSTM that processes sequences of team match history.

    Training
    --------
    Pass X_seq_train / X_seq_val as numpy arrays of shape
    (n_matches, seq_len, n_features) built with build_sequences().
    If omitted, falls back to seq_len=1 (single-timestep), so the model
    remains compatible with standard flat DataFrames.
    """

    MODEL_NAME = "LSTMBetModel"
    SUPPORTS_FEATURE_IMPORTANCE = True

    def __init__(
        self,
        sport:      str,
        market:     str,
        hidden:     int   = 128,
        n_layers:   int   = 2,
        dropout:    float = 0.30,
        epochs:     int   = 120,
        lr:         float = 1e-3,
        batch_size: int   = 256,
        patience:   int   = 15,
        seq_len:    int   = SEQ_LEN,
        model_dir:  Path | None = None,
    ):
        super().__init__(sport, market, model_dir)
        self.hidden, self.n_layers = hidden, n_layers
        self.dropout, self.epochs  = dropout, epochs
        self.lr, self.batch_size   = lr, batch_size
        self.patience, self.seq_len = patience, seq_len

        self._net:   _AttnBiLSTM | None           = None
        self._sc:    StandardScaler               = StandardScaler()
        self._le:    LabelEncoder                 = LabelEncoder()
        self._cals:  list[IsotonicRegression]     = []
        self._n_cls: int                          = 0

    # ── Sequence builder (static utility) ────────────────────────────────────

    @staticmethod
    def build_sequences(
        team_history: pd.DataFrame,
        feature_cols: list[str],
        match_ids:    list[int],
        seq_len:      int = SEQ_LEN,
    ) -> np.ndarray:
        """
        Build a 3-D look-back matrix from a single team's match history.

        Parameters
        ----------
        team_history : DataFrame sorted chronologically for ONE team.
                       Must contain a `match_id` column.
        feature_cols : columns to include in each timestep
        match_ids    : matches (in any order) to build sequences for
        seq_len      : how many prior matches to include

        Returns
        -------
        np.ndarray  shape (len(match_ids), seq_len, len(feature_cols))
        """
        id_to_row = {mid: i for i, mid in enumerate(team_history["match_id"].tolist())}
        vals      = team_history[feature_cols].values.astype(np.float32)
        F         = len(feature_cols)
        out       = np.zeros((len(match_ids), seq_len, F), dtype=np.float32)
        for j, mid in enumerate(match_ids):
            idx = id_to_row.get(mid)
            if idx is None:
                continue
            start = max(0, idx - seq_len)
            hist  = vals[start:idx]                   # shape (k, F), k ≤ seq_len
            pad   = seq_len - len(hist)
            out[j] = (np.vstack([np.zeros((pad, F), np.float32), hist])
                      if pad > 0 else hist)
        return out

    # ── helpers ──────────────────────────────────────────────────────────────

    def _scale_seq(self, X_flat: np.ndarray, X_seq: np.ndarray | None,
                   fit: bool = False) -> torch.Tensor:
        """Scale and tensorise sequences. Falls back to seq_len=1."""
        if fit:
            self._sc.fit(X_flat)
        if X_seq is not None:
            sh  = X_seq.shape
            sc  = self._sc.transform(X_seq.reshape(-1, sh[2])).reshape(sh)
        else:
            sc  = self._sc.transform(X_flat)[:, None, :]   # (N, 1, F)
        return torch.tensor(sc, dtype=torch.float32, device=DEVICE)

    def _proba_t(self, X_t: torch.Tensor) -> np.ndarray:
        self._net.eval()
        with torch.no_grad():
            return torch.softmax(self._net(X_t), 1).cpu().numpy()

    # ── train ────────────────────────────────────────────────────────────────

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val:   pd.DataFrame,
        y_val:   pd.Series,
        X_seq_train: np.ndarray | None = None,
        X_seq_val:   np.ndarray | None = None,
        **kwargs,
    ) -> TrainingResult:
        t0 = time.monotonic()
        self._classes       = sorted(y_train.unique().tolist())
        self._feature_names = X_train.columns.tolist()
        self._n_cls         = len(self._classes)
        in_dim              = len(self._feature_names)

        Xtr = self._scale_seq(X_train.values.astype(np.float32), X_seq_train, fit=True)
        Xva = self._scale_seq(X_val.values.astype(np.float32),   X_seq_val)
        ytr = torch.tensor(self._le.fit_transform(y_train).astype(np.int64),
                           dtype=torch.long, device=DEVICE)
        yva = torch.tensor(self._le.transform(y_val).astype(np.int64),
                           dtype=torch.long, device=DEVICE)

        self._net = _AttnBiLSTM(in_dim, self.hidden, self.n_layers,
                                self._n_cls, self.dropout).to(DEVICE)
        crit = nn.CrossEntropyLoss()
        opt  = torch.optim.Adam(self._net.parameters(), lr=self.lr, weight_decay=1e-4)
        n_b  = max(1, len(Xtr) // self.batch_size)
        sch  = torch.optim.lr_scheduler.OneCycleLR(
            opt, max_lr=self.lr * 10, steps_per_epoch=n_b,
            epochs=self.epochs, pct_start=0.15,
        )

        best_vloss, best_state, pctr = float("inf"), None, 0

        for ep in range(self.epochs):
            self._net.train()
            perm = torch.randperm(len(Xtr), device=DEVICE)
            for b in range(n_b):
                idx = perm[b * self.batch_size:(b + 1) * self.batch_size]
                opt.zero_grad()
                crit(self._net(Xtr[idx]), ytr[idx]).backward()
                nn.utils.clip_grad_norm_(self._net.parameters(), 1.0)
                opt.step()
                sch.step()

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
                    logger.info("[LSTM] Early-stop ep=%d val_loss=%.4f", ep, vl)
                    break

        if best_state:
            self._net.load_state_dict(best_state)

        self._fitted  = True
        val_proba     = self._proba_t(Xva)
        train_ll      = log_loss(self._le.transform(y_train), self._proba_t(Xtr))
        metrics       = self._compute_metrics(y_val, val_proba, self._classes)

        result = TrainingResult(
            model_name=self.MODEL_NAME, sport=self.sport, market=self.market,
            n_train=len(Xtr), n_val=len(Xva), n_features=in_dim,
            train_log_loss=round(train_ll, 4),
            best_params={"hidden": self.hidden, "n_layers": self.n_layers,
                         "seq_len": Xtr.shape[1], "dropout": self.dropout},
            duration_s=round(time.monotonic() - t0, 1),
            **{k: v for k, v in metrics.items() if k != "train_log_loss"},
        )
        result.train_log_loss = round(train_ll, 4)
        self._training_result = result
        logger.info("[LSTM/%s/%s]\n%s", self.sport, self.market, result.summary())
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
        Xsc = self._sc.transform(self._align_features(X_cal).values.astype(np.float32))
        X_t = torch.tensor(Xsc[:, None, :], dtype=torch.float32, device=DEVICE)
        raw = self._proba_t(X_t)
        n   = self._n_cls
        le_m = {c: i for i, c in enumerate(self._classes)}
        y_e  = y_cal.map(le_m).fillna(0).astype(int).values
        y_oh = np.eye(n)[y_e]
        self._cals = []
        for i in range(n):
            iso = IsotonicRegression(out_of_bounds="clip")
            iso.fit(raw[:, i], y_oh[:, i])
            self._cals.append(iso)
        self._calibrated = True
        logger.info("[LSTM] Calibrated on %d samples", len(X_cal))

    def _apply_cal(self, p: np.ndarray) -> np.ndarray:
        if not self._calibrated or not self._cals:
            return p
        cols = np.column_stack([c.predict(p[:, i]) for i, c in enumerate(self._cals)])
        s    = cols.sum(axis=1, keepdims=True)
        return cols / np.where(s == 0, 1, s)

    # ── predict_proba ────────────────────────────────────────────────────────

    def predict_proba(
        self,
        X:     pd.DataFrame,
        X_seq: np.ndarray | None = None,
    ) -> PredictionBatch:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        with self._predict_lock:
            flat = self._align_features(X).values.astype(np.float32)
            X_t  = self._scale_seq(flat, X_seq)
            raw  = self._proba_t(X_t)
            p    = self._apply_cal(raw)
            df   = pd.DataFrame(p, columns=self._classes)
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
            "net":     self._net.state_dict() if self._net else {},
            "sc":      self._sc, "le": self._le, "cals": self._cals,
            "hidden":  self.hidden, "n_layers": self.n_layers,
            "dropout": self.dropout, "seq_len": self.seq_len,
            "n_cls":   self._n_cls, "clss":  self._classes,
            "feats":   self._feature_names, "cal": self._calibrated,
        }, tmp)
        tmp.rename(path)
        self._write_metadata_sidecar(path)
        logger.info("[LSTM] Saved → %s", path)
        return path

    def load(self, path: Path) -> None:
        self._read_metadata_sidecar(path)
        c = torch.load(path, map_location="cpu")
        self.hidden, self.n_layers   = c["hidden"], c["n_layers"]
        self.dropout, self.seq_len   = c["dropout"], c["seq_len"]
        self._n_cls, self._classes   = c["n_cls"], c["clss"]
        self._feature_names          = c["feats"]
        self._sc, self._le           = c["sc"], c["le"]
        self._cals                   = c.get("cals", [])
        self._calibrated             = c.get("cal", False)
        in_dim   = len(self._feature_names)
        self._net = _AttnBiLSTM(in_dim, self.hidden, self.n_layers,
                                self._n_cls, self.dropout).to(DEVICE)
        self._net.load_state_dict(c["net"])
        self._net.eval()
        self._fitted = True

    # ── feature importance (gradient sensitivity) ─────────────────────────────

    def get_feature_importance(self, top_n: int = 30) -> pd.DataFrame:
        if not self._fitted:
            raise RuntimeError("Model not fitted")
        x = torch.zeros(1, 1, len(self._feature_names),
                        requires_grad=True, device=DEVICE)
        self._net.eval()
        self._net(x).sum().backward()
        imp = x.grad.abs().squeeze(-3).squeeze(0).detach().cpu().numpy()
        df  = (
            pd.DataFrame({"feature": self._feature_names, "importance": imp})
            .sort_values("importance", ascending=False)
            .head(top_n)
            .reset_index(drop=True)
        )
        df["rank"] = df.index + 1
        return df
