"""
accumulator_builder.py — Builds 3-leg, 5-leg, and 10-leg accumulators.

Strategy
--------
1. Fetch all predictions for upcoming matches with status=PENDING
2. Filter to high-confidence, positive-edge selections
3. Optimise selection order (highest confidence first)
4. Compute combined odds and Kelly criterion recommended stake
5. Persist to Supabase accumulators + accumulator_legs tables
6. Checks correlation between selections (avoid same-league/same-day stacking)
7. Runs as on-demand function and daily scheduled job

Accumulator types
-----------------
- 3odds:  3 selections — good for conservative punters
- 5odds:  5 selections — medium risk/reward
- 10odds: 10 selections — high-risk, high-reward (lottery style)

Kelly Criterion
---------------
f* = (bp - q) / b
where b = decimal_odds - 1, p = model_prob, q = 1 - p

We apply a fractional Kelly (25%) for bankroll safety.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..database import get_supabase_admin   # or pass in from caller

logger = logging.getLogger(__name__)

# ─────────────────────────── Config ────────────────────────────────────────

ACCA_CONFIGS = {
    "3odds":  {"legs": 3,  "min_edge": 0.03, "min_confidence": 55.0},
    "5odds":  {"legs": 5,  "min_edge": 0.02, "min_confidence": 50.0},
    "10odds": {"legs": 10, "min_edge": 0.01, "min_confidence": 45.0},
}

KELLY_FRACTION  = 0.25   # fractional Kelly (25%)
MIN_ODDS        = 1.20   # ignore selections shorter than this
MAX_SINGLE_ODDS = 5.00   # cap runaway long shots in accumulators
MAX_SAME_LEAGUE = 2      # max selections from same league in one acca
MAX_SAME_DATE   = 4      # max selections from same calendar date


# ─────────────────────────── Data Classes ──────────────────────────────────

@dataclass
class Selection:
    prediction_id:   int
    match_id:        int
    market:          str
    predicted_outcome: str
    model_probability: float
    odds:            float
    confidence_score: float
    edge:            float
    league_id:       int | None
    match_date:      str

    def kelly_fraction(self) -> float:
        """Fractional Kelly stake as % of bankroll."""
        b = self.odds - 1
        p = self.model_probability
        q = 1 - p
        f_full = (b * p - q) / b if b > 0 else 0.0
        return max(0.0, round(f_full * KELLY_FRACTION * 100, 2))


@dataclass
class AccumulatorResult:
    acca_type:    str           # "3odds" | "5odds" | "10odds"
    selections:   list[Selection]
    combined_odds: float
    kelly_stake:  float
    expected_value: float
    status:       str = "PENDING"

    def to_db_rows(self) -> tuple[dict, list[dict]]:
        """Return (accumulator row, [leg rows])."""
        acca_row = {
            "acca_type":    self.acca_type,
            "total_odds":   round(self.combined_odds, 3),
            "status":       self.status,
            "created_at":   datetime.now(timezone.utc).isoformat(),
        }
        leg_rows = [
            {
                "prediction_id": s.prediction_id,
                "status":        "PENDING",
            }
            for s in self.selections
        ]
        return acca_row, leg_rows


# ─────────────────────────── Correlation Filter ────────────────────────────

class CorrelationFilter:
    """
    Prevents over-correlated accumulators:
    - Same league: max MAX_SAME_LEAGUE selections
    - Same date:   max MAX_SAME_DATE selections
    - Exact same match: never pick two markets from the same match
    """

    def __init__(self, selections: list[Selection]):
        self._selections = selections

    def filter(self) -> list[Selection]:
        seen_matches:  set[int]         = set()
        league_count:  dict[int, int]   = {}
        date_count:    dict[str, int]   = {}
        valid:         list[Selection]  = []

        for s in self._selections:
            if s.match_id in seen_matches:
                continue
            lid = s.league_id or -1
            dt  = s.match_date[:10]
            if league_count.get(lid, 0) >= MAX_SAME_LEAGUE:
                continue
            if date_count.get(dt, 0) >= MAX_SAME_DATE:
                continue
            valid.append(s)
            seen_matches.add(s.match_id)
            league_count[lid]  = league_count.get(lid, 0) + 1
            date_count[dt]     = date_count.get(dt, 0) + 1

        return valid


# ─────────────────────────── Builder ───────────────────────────────────────

class AccumulatorBuilder:

    def __init__(self, supabase_client=None):
        self.client = supabase_client or get_supabase_admin()

    # ── Public API ──────────────────────────────────────────────

    def run(self) -> list[AccumulatorResult]:
        """Build all three accumulator types and persist them."""
        logger.info("🎰 AccumulatorBuilder starting …")
        pool = self._load_candidate_selections()
        logger.info("Candidate pool: %d selections", len(pool))

        if len(pool) < 3:
            logger.warning("Not enough selections to build accumulators")
            return []

        results: list[AccumulatorResult] = []
        for acca_type, cfg in ACCA_CONFIGS.items():
            try:
                acca = self._build(acca_type, pool, cfg)
                if acca:
                    self._persist(acca)
                    results.append(acca)
                    logger.info(
                        "✅ %s built — odds=%.2f  EV=%.4f  kelly=%.1f%%",
                        acca_type, acca.combined_odds, acca.expected_value, acca.kelly_stake,
                    )
            except Exception as exc:
                logger.error("Failed to build %s: %s", acca_type, exc, exc_info=True)

        return results

    def build_single_type(self, acca_type: str) -> AccumulatorResult | None:
        """Build one accumulator type on demand."""
        if acca_type not in ACCA_CONFIGS:
            raise ValueError(f"Unknown acca_type '{acca_type}'")
        pool = self._load_candidate_selections()
        cfg  = ACCA_CONFIGS[acca_type]
        acca = self._build(acca_type, pool, cfg)
        if acca:
            self._persist(acca)
        return acca

    # ── Core build ──────────────────────────────────────────────

    def _build(self, acca_type: str, pool: list[Selection], cfg: dict) -> AccumulatorResult | None:
        n_legs    = cfg["legs"]
        min_edge  = cfg["min_edge"]
        min_conf  = cfg["min_confidence"]

        # Step 1: filter to eligible selections
        eligible = [
            s for s in pool
            if s.edge >= min_edge
            and s.confidence_score >= min_conf
            and MIN_ODDS <= s.odds <= MAX_SINGLE_ODDS
        ]

        if len(eligible) < n_legs:
            logger.warning("[%s] Only %d eligible after filter (need %d)",
                           acca_type, len(eligible), n_legs)
            return None

        # Step 2: de-correlate
        eligible = CorrelationFilter(eligible).filter()

        if len(eligible) < n_legs:
            logger.warning("[%s] Only %d after correlation filter (need %d)",
                           acca_type, len(eligible), n_legs)
            return None

        # Step 3: rank by composite score and take top N
        # Composite: confidence (50%) + edge (30%) + model_prob distance from 0.5 (20%)
        eligible.sort(key=lambda s: (
            0.5 * s.confidence_score / 100
            + 0.3 * min(s.edge / 0.15, 1.0)
            + 0.2 * (abs(s.model_probability - 0.5) * 2)
        ), reverse=True)

        selections = eligible[:n_legs]

        # Step 4: combined odds
        combined_odds = 1.0
        for s in selections:
            combined_odds *= s.odds
        combined_odds = round(combined_odds, 3)

        # Step 5: Kelly on the whole acca (conservative)
        avg_prob  = sum(s.model_probability for s in selections) ** (1.0 / n_legs)
        b_acca    = combined_odds - 1
        q_acca    = 1 - avg_prob
        f_full    = (b_acca * avg_prob - q_acca) / b_acca if b_acca > 0 else 0.0
        kelly_pct = round(max(0.0, f_full * KELLY_FRACTION * 100), 2)

        # Step 6: expected value
        ev = avg_prob * combined_odds - 1.0

        return AccumulatorResult(
            acca_type=acca_type,
            selections=selections,
            combined_odds=combined_odds,
            kelly_stake=kelly_pct,
            expected_value=round(ev, 4),
        )

    # ── DB I/O ──────────────────────────────────────────────────

    def _load_candidate_selections(self) -> list[Selection]:
        """
        Load all PENDING predictions for upcoming matches.
        Join match data for league and date correlation filtering.
        """
        res = (
            self.client.table("predictions")
            .select(
                "id, match_id, market, predicted_outcome, "
                "model_probability, odds, confidence_score, edge, "
                "matches!inner(match_date, league_id, status)"
            )
            .eq("status", "PENDING")
            .eq("matches.status", "upcoming")
            .gt("edge", 0.0)
            .gt("odds", MIN_ODDS)
            .order("confidence_score", desc=True)
            .execute()
        )
        rows = res.data or []
        selections: list[Selection] = []
        for r in rows:
            match_data = r.get("matches") or {}
            try:
                selections.append(Selection(
                    prediction_id=r["id"],
                    match_id=r["match_id"],
                    market=r["market"],
                    predicted_outcome=r["predicted_outcome"],
                    model_probability=float(r.get("model_probability", 0.5)),
                    odds=float(r.get("odds", 2.0)),
                    confidence_score=float(r.get("confidence_score", 50)),
                    edge=float(r.get("edge", 0)),
                    league_id=match_data.get("league_id"),
                    match_date=match_data.get("match_date", ""),
                ))
            except (KeyError, TypeError, ValueError) as exc:
                logger.debug("Skipping invalid selection row: %s", exc)
        return selections

    def _persist(self, acca: AccumulatorResult) -> None:
        """Insert accumulator and its legs into Supabase."""
        acca_row, leg_payloads = acca.to_db_rows()
        try:
            res = self.client.table("accumulators").insert(acca_row).execute()
            acca_id = res.data[0]["id"] if res.data else None
            if acca_id and leg_payloads:
                for leg in leg_payloads:
                    leg["accumulator_id"] = acca_id
                self.client.table("accumulator_legs").insert(leg_payloads).execute()
                logger.info("Persisted %s (id=%d) with %d legs",
                            acca.acca_type, acca_id, len(leg_payloads))
        except Exception as exc:
            logger.error("Failed to persist accumulator: %s", exc, exc_info=True)

    # ── Return-on-investment tracker ─────────────────────────────

    def roi_report(self, lookback_days: int = 90) -> dict[str, Any]:
        """
        Fetch recent resolved accumulators and calculate ROI per type.
        Returns dict with per-type stats.
        """
        from datetime import timedelta
        cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).isoformat()
        res = (
            self.client.table("accumulators")
            .select("acca_type, total_odds, status, created_at")
            .in_("status", ["WON", "LOST"])
            .gte("created_at", cutoff)
            .execute()
        )
        rows = res.data or []

        report: dict[str, Any] = {}
        for acca_type in ACCA_CONFIGS:
            subset = [r for r in rows if r["acca_type"] == acca_type]
            if not subset:
                report[acca_type] = {"bets": 0, "won": 0, "roi": None}
                continue
            won  = sum(1 for r in subset if r["status"] == "WON")
            bets = len(subset)
            # Assume uniform unit stakes (1 unit per acca)
            total_return = sum(r["total_odds"] for r in subset if r["status"] == "WON")
            roi = (total_return - bets) / bets if bets > 0 else 0.0
            report[acca_type] = {
                "bets": bets,
                "won":  won,
                "win_rate": round(won / bets, 4),
                "roi": round(roi, 4),
            }

        return report


# ─────────────────────────── Scheduler Entry Point ─────────────────────────

def run_accumulator_job() -> list[AccumulatorResult]:
    """
    Called by APScheduler after daily fixtures + predictions are ready.
    Expects predictions table to have fresh PENDING rows.
    """
    builder = AccumulatorBuilder()
    results = builder.run()
    logger.info("AccumulatorBuilder job complete — %d accas created", len(results))
    return results


if __name__ == "__main__":
    import logging as _logging
    _logging.basicConfig(level=_logging.INFO)
    run_accumulator_job()
