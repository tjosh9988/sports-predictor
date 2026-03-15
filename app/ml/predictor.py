"""
predictor.py — Live prediction engine for Bet Hero.

Workflow per match
------------------
1. Build feature vector via FeatureEngineer.build(match_id)
2. Load the EnsembleModel for the sport + market
3. Get calibrated P(Home), P(Draw), P(Away)
4. Convert odds → implied probability (overround-stripped)
5. Compute edge = model_prob - implied_prob
6. Assign confidence score (0–100)
7. Persist prediction row to Supabase predictions table
8. Return PredictionResult dataclass

Markets supported
-----------------
- 1X2     (Home / Draw / Away)
- BTTS    (Yes / No)
- Over2.5 (Yes / No)
- Handicap (Home -1 / Away -1)

Caching
-------
EnsembleModel objects are cached in-process by (sport, market) key.
Feature vectors are not cached (always fresh before match).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .feature_engineer import FeatureEngineer
from .models import EnsembleModel, MODEL_DIR, CLASSES, BINARY_CLS

logger = logging.getLogger(__name__)

# ─────────────────────────── Data Classes ──────────────────────────────────

@dataclass
class PredictionResult:
    match_id:           int
    market:             str
    predicted_outcome:  str
    model_probability:  float   # P(predicted outcome), calibrated
    implied_probability:float   # from closing odds, overround-stripped
    edge:               float   # model_prob - implied_prob
    odds:               float   # closing decimal odds for predicted outcome
    confidence_score:   float   # 0-100 composite confidence
    status:             str = "PENDING"
    created_at:         str = ""

    def to_db_row(self) -> dict:
        d = asdict(self)
        d.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        return d


@dataclass
class OddsLine:
    """Normalised odds line for a specific market."""
    market:   str
    home:     float | None = None
    draw:     float | None = None
    away:     float | None = None

    def implied_prob(self, outcome: str) -> float:
        """Overround-stripped implied probability for requested outcome."""
        raw_home = 1 / self.home if self.home else 0.0
        raw_draw = 1 / self.draw if self.draw else 0.0
        raw_away = 1 / self.away if self.away else 0.0
        overround = raw_home + raw_draw + raw_away
        mapping = {
            "Home": raw_home,
            "Draw": raw_draw,
            "Away": raw_away,
            "Yes":  raw_home,   # BTTS / Over: home slot = Yes
            "No":   raw_away,
        }
        raw = mapping.get(outcome, 0.0)
        return (raw / overround) if overround > 0 else 0.0

    def decimal_odds(self, outcome: str) -> float:
        mapping = {
            "Home": self.home or 0.0,
            "Draw": self.draw or 0.0,
            "Away": self.away or 0.0,
            "Yes":  self.home or 0.0,
            "No":   self.away or 0.0,
        }
        return mapping.get(outcome, 0.0)


# ─────────────────────────── Model Cache ───────────────────────────────────

class _ModelCache:
    _store: dict[str, EnsembleModel] = {}

    @classmethod
    def get(cls, sport: str, market: str) -> EnsembleModel | None:
        key = f"{sport}::{market}"
        if key not in cls._store:
            path = MODEL_DIR / f"EnsembleMeta_{sport}_{market}.joblib"
            if path.exists():
                try:
                    cls._store[key] = EnsembleModel.load(sport, market)
                    logger.info("Loaded ensemble model %s", key)
                except Exception as exc:
                    logger.error("Failed to load model %s: %s", key, exc)
                    return None
            else:
                logger.warning("No trained model for %s — skipping", key)
                return None
        return cls._store[key]

    @classmethod
    def invalidate(cls, sport: str, market: str) -> None:
        cls._store.pop(f"{sport}::{market}", None)


# ─────────────────────────── Confidence Scorer ─────────────────────────────

def _confidence_score(
    model_prob: float,
    implied_prob: float,
    edge: float,
    overround: float,
    feature_completeness: float,   # 0–1: fraction of features that were non-zero
) -> float:
    """
    Composite confidence score in [0, 100].

    Components
    ----------
    - Edge magnitude:        higher edge = higher confidence (40% weight)
    - Model probability:     extreme probabilities → more confident (20%)
    - Market efficiency:     low overround = efficient/trusted market (20%)
    - Feature completeness:  more valid features = more confident (20%)
    """
    # Edge component (cap at edge of 0.15 = full score)
    edge_score = min(1.0, max(0.0, edge / 0.15)) * 40.0

    # Model probability: distance from 0.5 → certainty
    prob_score = min(1.0, abs(model_prob - 0.5) * 2) * 20.0

    # Market efficiency: overround < 1.04 is good, > 1.12 is bad
    eff = max(0.0, min(1.0, (1.12 - overround) / (1.12 - 1.02)))
    market_score = eff * 20.0

    # Feature completeness
    feat_score = feature_completeness * 20.0

    total = edge_score + prob_score + market_score + feat_score
    return round(min(100.0, max(0.0, total)), 2)


# ─────────────────────────── Predictor ─────────────────────────────────────

class Predictor:
    """
    Generates predictions for one or many matches and persists them to Supabase.
    """

    SUPPORTED_MARKETS = ["1X2", "BTTS", "Over2.5"]

    def __init__(self, supabase_client):
        self.client = supabase_client
        self._fe    = FeatureEngineer(supabase_client)

    # ── Public API ──────────────────────────────────────────────

    def predict_match(
        self,
        match_id: int,
        markets: list[str] | None = None,
        persist: bool = True,
    ) -> list[PredictionResult]:
        """
        Generate predictions for all requested markets for a single match.
        Returns list of PredictionResult objects.
        """
        markets = markets or self.SUPPORTED_MARKETS
        match   = self._load_match(match_id)
        if not match:
            logger.error("Match %d not found", match_id)
            return []

        sport_slug = (match.get("sports") or {}).get("slug", "football")

        # Build features once — reuse for all markets
        try:
            raw_features = self._fe.build(match_id)
        except Exception as exc:
            logger.error("Feature build failed for match %d: %s", match_id, exc)
            return []

        X = pd.DataFrame([raw_features]).fillna(0.0)
        feat_completeness = float((X != 0).mean().mean())

        # Load odds for this match
        odds_data = self._load_odds(match_id)

        results: list[PredictionResult] = []
        for market in markets:
            try:
                result = self._predict_market(
                    match_id, sport_slug, market, X, odds_data, feat_completeness
                )
                if result:
                    results.append(result)
            except Exception as exc:
                logger.error("Prediction failed match=%d market=%s: %s", match_id, market, exc)

        if persist and results:
            self._persist(results)

        return results

    def predict_upcoming(
        self,
        sport_slug: str | None = None,
        hours_ahead: int = 48,
        markets: list[str] | None = None,
    ) -> list[PredictionResult]:
        """Predict all upcoming matches in the next *hours_ahead* hours."""
        from datetime import timedelta
        now    = datetime.now(timezone.utc)
        cutoff = (now + timedelta(hours=hours_ahead)).isoformat()

        q = (
            self.client.table("matches")
            .select("id, sport_id, sports!inner(slug)")
            .eq("status", "upcoming")
            .gte("match_date", now.isoformat())
            .lte("match_date", cutoff)
        )
        if sport_slug:
            q = q.eq("sports.slug", sport_slug)

        res = q.execute()
        match_ids = [r["id"] for r in (res.data or [])]
        logger.info("Predicting %d upcoming matches …", len(match_ids))

        all_results: list[PredictionResult] = []
        for mid in match_ids:
            preds = self.predict_match(mid, markets=markets, persist=True)
            all_results.extend(preds)
        return all_results

    # ── Core market prediction ───────────────────────────────────

    def _predict_market(
        self,
        match_id: int,
        sport_slug: str,
        market: str,
        X: pd.DataFrame,
        odds_data: list[dict],
        feat_completeness: float,
    ) -> PredictionResult | None:

        model = _ModelCache.get(sport_slug, market)
        if model is None:
            logger.debug("No model for %s/%s", sport_slug, market)
            return None

        # Align features to model's expected columns
        X_aligned = self._align_features(X, sport_slug, market)

        # Probabilities
        proba = model.predict_proba(X_aligned)   # shape (1, n_classes)
        proba_row = proba[0]
        classes = model._classes

        # Best outcome
        best_idx     = int(proba_row.argmax())
        predicted    = classes[best_idx]
        model_prob   = float(proba_row[best_idx])

        # Odds line
        odds_line = self._extract_odds_line(market, odds_data)
        implied   = odds_line.implied_prob(predicted)
        dec_odds  = odds_line.decimal_odds(predicted)

        edge = round(model_prob - implied, 4)

        # Overround for confidence
        raw_probs = [(1 / odds_line.home) if odds_line.home else 0,
                     (1 / odds_line.draw) if odds_line.draw else 0,
                     (1 / odds_line.away) if odds_line.away else 0]
        overround = sum(raw_probs) or 1.02

        confidence = _confidence_score(model_prob, implied, edge, overround, feat_completeness)

        return PredictionResult(
            match_id=match_id,
            market=market,
            predicted_outcome=predicted,
            model_probability=round(model_prob, 4),
            implied_probability=round(implied, 4),
            edge=edge,
            odds=round(dec_odds, 3),
            confidence_score=confidence,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    # ── Helpers ─────────────────────────────────────────────────

    def _align_features(self, X: pd.DataFrame, sport: str, market: str) -> pd.DataFrame:
        """
        Ensure feature columns match the trained model's expected columns.
        Missing columns → 0.0, extra columns → dropped.
        """
        meta_path = MODEL_DIR / f"feature_names_{sport}_{market}.joblib"
        if meta_path.exists():
            import joblib
            expected_cols = joblib.load(meta_path)
            missing = [c for c in expected_cols if c not in X.columns]
            extra   = [c for c in X.columns     if c not in expected_cols]
            for col in missing:
                X[col] = 0.0
            X = X.drop(columns=extra, errors="ignore")
            X = X[expected_cols]   # correct column order
        return X.fillna(0.0)

    def _load_match(self, match_id: int) -> dict | None:
        res = (
            self.client.table("matches")
            .select("*, sports(slug)")
            .eq("id", match_id)
            .single()
            .execute()
        )
        return res.data

    def _load_odds(self, match_id: int) -> list[dict]:
        res = (
            self.client.table("odds_history")
            .select("*")
            .eq("match_id", match_id)
            .execute()
        )
        return res.data or []

    def _extract_odds_line(self, market: str, odds_data: list[dict]) -> OddsLine:
        """Pick Pinnacle closing odds for the market; fallback to best available."""
        # Market aliases
        market_aliases = {
            "1X2":      ["1X2", "h2h", "match_winner"],
            "BTTS":     ["BTTS", "btts", "both_teams_to_score"],
            "Over2.5":  ["Over/Under", "totals", "over_under"],
        }
        aliases = market_aliases.get(market, [market])
        candidates = [r for r in odds_data
                      if any(a.lower() in (r.get("market") or "").lower() for a in aliases)]

        if not candidates:
            return OddsLine(market=market)

        # Prefer Pinnacle
        pinnacle = next((r for r in candidates
                         if "pinnacle" in (r.get("bookmaker") or "").lower()), None)
        row = pinnacle or candidates[0]

        return OddsLine(
            market=market,
            home=row.get("closing_home") or row.get("opening_home"),
            draw=row.get("closing_draw") or row.get("opening_draw"),
            away=row.get("closing_away") or row.get("opening_away"),
        )

    def _persist(self, results: list[PredictionResult]) -> None:
        rows = [r.to_db_row() for r in results]
        try:
            self.client.table("predictions").upsert(
                rows,
                on_conflict="match_id,market",
            ).execute()
            logger.info("Persisted %d predictions", len(rows))
        except Exception as exc:
            logger.error("Failed to persist predictions: %s", exc, exc_info=True)
