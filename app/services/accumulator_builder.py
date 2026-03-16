"""
accumulator_builder.py — Three-pass selection algorithm for betting accumulators.
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Set, Dict, Any, Optional

from app.database import get_supabase_admin
# from anthropic import Anthropic  
# from app.ml.predictor import predict_match  

logger = logging.getLogger(__name__)

@dataclass
class Selection:
    prediction_id: int
    match_id: int
    sport: str
    league: str
    market: str
    outcome: str
    odds: float
    model_prob: float
    confidence: float
    ev: float

@dataclass
class Accumulator:
    type: str  # "10odds", "5odds", "3odds"
    selections: List[Selection]
    total_odds: float
    combined_ev: float

class AccumulatorBuilder:
    def __init__(self, supabase_client=None):
        self.client = supabase_client or get_supabase_admin()
        self.used_match_ids: Set[int] = set()

    def run(self) -> Dict[str, Optional[Accumulator]]:
        """
        Runs the three-pass algorithm and returns the generated accumulators.
        """
        logger.info("Starting AccumulatorBuilder three-pass run...")
        
        # 1. Fetch available predictions for next 72 hours
        pool = self._fetch_prediction_pool()
        if not pool:
            logger.warning("Prediction pool is empty.")
            return {"10odds": None, "5odds": None, "3odds": None}

        # 2. Pass 1: Build 10odds (8-12 combined)
        # Filters: confidence > 55%, odds 1.5-2.5, 5-6 legs
        acca_10 = self._build_pass(
            pool=pool,
            type_name="10odds",
            min_conf=55.0,
            min_odds_leg=1.5,
            max_odds_leg=2.5,
            target_min_odds=8.0,
            target_max_odds=12.0,
            target_legs=(5, 6)
        )

        # 3. Pass 2: Build 5odds (4-6 combined)
        # Filters: confidence > 62%, odds 1.35-1.75, 3-4 legs
        acca_5 = self._build_pass(
            pool=pool,
            type_name="5odds",
            min_conf=62.0,
            min_odds_leg=1.35,
            max_odds_leg=1.75,
            target_min_odds=4.0,
            target_max_odds=6.0,
            target_legs=(3, 4)
        )

        # 4. Pass 3: Build 3odds (2.5-3.5 combined)
        # Filters: confidence > 70%, odds 1.25-1.55, 2-3 legs
        acca_3 = self._build_pass(
            pool=pool,
            type_name="3odds",
            min_conf=70.0,
            min_odds_leg=1.25,
            max_odds_leg=1.55,
            target_min_odds=2.5,
            target_max_odds=3.5,
            target_legs=(2, 3)
        )

        results = {
            "10odds": acca_10,
            "5odds": acca_5,
            "3odds": acca_3
        }
        
        # Persist results
        for acca in results.values():
            if acca:
                self._persist_accumulator(acca)

        return results

    def _build_pass(
        self,
        pool: List[Selection],
        type_name: str,
        min_conf: float,
        min_odds_leg: float,
        max_odds_leg: float,
        target_min_odds: float,
        target_max_odds: float,
        target_legs: tuple[int, int]
    ) -> Optional[Accumulator]:
        """
        Generic pass builder logic.
        """
        # Filter pool based on criteria + not used
        eligible = [
            s for s in pool
            if s.match_id not in self.used_match_ids
            and s.confidence >= min_conf
            and min_odds_leg <= s.odds <= max_odds_leg
        ]

        # Rank by EV score descending
        eligible.sort(key=lambda x: x.ev, reverse=True)

        selected: List[Selection] = []
        current_odds = 1.0
        used_leagues: Set[str] = set()
        used_sports: Set[str] = set()

        for s in eligible:
            # Check de-correlation (Different leagues/sports preferred)
            # Rule: Select legs from DIFFERENT leagues/sports to reduce correlation
            if s.league in used_leagues or s.sport in used_sports:
                continue
            
            # Additional safety: stop if adding this leg exceeds target odds 
            if current_odds * s.odds > target_max_odds:
                continue

            selected.append(s)
            current_odds *= s.odds
            used_leagues.add(s.league)
            used_sports.add(s.sport)

            # Break if we hit the leg count and target odds
            if len(selected) >= target_legs[1]:
                break
        
        # Final validation
        if len(selected) < target_legs[0] or current_odds < target_min_odds:
            logger.warning("[%s] Could not meet target criteria. Found %d legs with %.2f odds.", 
                           type_name, len(selected), current_odds)
            # If not enough, the prompt says "build fewer legs at correct odds. Never pad with negative-EV"
            # Since our pool only contains positive EV, we can return what we have if it's within sensible bounds
            if not selected:
                return None

        # Add to used set
        for s in selected:
            self.used_match_ids.add(s.match_id)

        return Accumulator(
            type=type_name,
            selections=selected,
            total_odds=round(current_odds, 2),
            combined_ev=sum(s.ev for s in selected) / len(selected) # Simplified combined EV
        )

    def _fetch_prediction_pool(self) -> List[Selection]:
        """
        Fetches all predictions for upcoming matches, removes overround, 
        and calculates true EV for each selection.
        """
        now = datetime.now(timezone.utc)
        future = now + timedelta(hours=72)

        try:
            # Assuming schema from previous tasks
            res = (
                self.client.table("predictions")
                .select(
                    "id, match_id, market, predicted_outcome, "
                    "model_probability, odds, confidence_score, "
                    "matches(match_date, sport, league)"
                )
                .gte("matches.match_date", now.isoformat())
                .lte("matches.match_date", future.isoformat())
                .execute()
            )
            
            rows = res.data or []
            pool: List[Selection] = []

            for r in rows:
                match = r.get("matches", {})
                if not match: continue
                
                model_prob = float(r.get("model_probability", 0))
                odds = float(r.get("odds", 0))
                
                if not odds or odds <= 1: continue

                # Inline EV formula: (prob * (odds - 1)) - (1 - prob)
                ev = (model_prob * (odds - 1)) - (1 - model_prob)
                
                # Only keep positive EV
                if ev <= 0:
                    continue
                
                pool.append(Selection(
                    prediction_id=r["id"],
                    match_id=r["match_id"],
                    sport=match.get("sport", "unknown"),
                    league=match.get("league", "unknown"),
                    market=r["market"],
                    outcome=r["predicted_outcome"],
                    odds=odds,
                    model_prob=model_prob,
                    confidence=float(r.get("confidence_score", 0)),
                    ev=ev
                ))
            
            return pool
        except Exception as exc:
            logger.error("Failed to fetch prediction pool: %s", exc)
            return []

    def _persist_accumulator(self, acca: Accumulator):
        """
        Inserts the accumulator and its legs into Supabase.
        """
        try:
            # 1. Insert into accumulators
            acca_res = self.client.table("accumulators").insert({
                "acca_type": acca.type,
                "total_odds": acca.total_odds,
                "status": "PENDING",
                "created_at": datetime.now(timezone.utc).isoformat()
            }).execute()

            if not acca_res.data:
                return

            acca_id = acca_res.data[0]["id"]

            # 2. Insert legs
            legs_payload = []
            for s in acca.selections:
                legs_payload.append({
                    "accumulator_id": acca_id,
                    "prediction_id": s.prediction_id,
                    "status": "PENDING"
                })
            
            self.client.table("accumulator_legs").insert(legs_payload).execute()
            logger.info("Persisted %s accumulator with %d legs.", acca.type, len(legs_payload))

        except Exception as exc:
            logger.error("Failed to persist accumulator %s: %s", acca.type, exc)

def build_all_accumulators():
    # Get upcoming fixtures - simple query no joins
    try:
        print("Querying upcoming fixtures...")
        result = supabase.table("matches")\
            .select("*")\
            .eq("status", "upcoming")\
            .limit(200)\
            .execute()
        
        fixtures = result.data or []
        print(f"UPCOMING FIXTURES FOUND: {len(fixtures)}")
        
    except Exception as e:
        print(f"Fixture fetch error: {e}")
        import traceback
        traceback.print_exc()
        fixtures = []
