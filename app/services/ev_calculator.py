"""
ev_calculator.py — Expected Value (EV) calculation service.

Calculates the Expected Value of a bet by comparing model-derived probabilities 
against market-implied probabilities (with bookmaker overround removed).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)

@dataclass
class MarketOdds:
    """Raw decimal odds from a bookmaker."""
    home: float
    draw: Optional[float] = None
    away: Optional[float] = None

@dataclass
class EVCalculation:
    """Result of an EV calculation for a specific selection."""
    selection: str          # e.g., "Home", "Draw", "Away", "Yes", "No"
    odds: float             # Raw decimal odds
    model_prob: float       # Calibrated model probability
    market_prob: float      # Overround-stripped market probability
    ev: float               # Expected Value score
    edge: float             # model_prob - market_prob

class EVCalculator:
    """
    Handles overround removal and EV computation for betting markets.
    """

    @staticmethod
    def calculate_true_probabilities(odds: MarketOdds) -> dict[str, float]:
        """
        Removes the bookmaker's overround using the Proportional Method (standard).
        Returns a mapping of selection to its 'true' market-implied probability.
        """
        # Sum of implied probabilities (including overround)
        raw_home = 1.0 / odds.home if odds.home > 0 else 0
        raw_draw = 1.0 / odds.draw if (odds.draw and odds.draw > 0) else 0
        raw_away = 1.0 / odds.away if (odds.away and odds.away > 0) else 0
        
        total_implied = raw_home + raw_draw + raw_away
        
        if total_implied == 0:
            return {}

        true_probs = {
            "Home": raw_home / total_implied,
            "Away": raw_away / total_implied
        }
        if odds.draw:
            true_probs["Draw"] = raw_draw / total_implied
            
        return true_probs

    @staticmethod
    def calculate_ev(model_prob: float, odds: float) -> float:
        """
        EV = (Probability of Winning * Amount Won per Unit) - (Probability of Losing * Unit Stake)
           = (model_prob * (odds - 1)) - (1 - model_prob)
        """
        if odds <= 1.0:
            return -1.0
        return (model_prob * (odds - 1.0)) - (1.0 - model_prob)

    def get_value_selections(
        self, 
        model_probs: dict[str, float], 
        market_odds: MarketOdds
    ) -> List[EVCalculation]:
        """
        Computes EV for all outcomes in a market and returns only those where EV > 0.
        Ranked by EV score descending.
        """
        true_market_probs = self.calculate_true_probabilities(market_odds)
        results = []

        mapping = {
            "Home": market_odds.home,
            "Draw": market_odds.draw,
            "Away": market_odds.away
        }

        for selection, m_prob in model_probs.items():
            odds = mapping.get(selection)
            if not odds or odds <= 1.0:
                continue
                
            market_prob = true_market_probs.get(selection, 0.0)
            ev = self.calculate_ev(m_prob, odds)
            edge = m_prob - market_prob

            if ev > 0:
                results.append(EVCalculation(
                    selection=selection,
                    odds=odds,
                    model_prob=m_prob,
                    market_prob=market_prob,
                    ev=round(ev, 4),
                    edge=round(edge, 4)
                ))

        # Rank by EV score descending
        return sorted(results, key=lambda x: x.ev, reverse=True)
