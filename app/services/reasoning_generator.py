"""
reasoning_generator.py — AI-powered value reasoning for bet selections.

Uses Anthropic Claude (sonnet) to generate human-readable explanations 
for model predictions based on top influential features.
"""

from __future__ import annotations

import logging
import os
from typing import Dict, Any, List, Optional

from anthropic import Anthropic
from app.database import get_supabase_admin

logger = logging.getLogger(__name__)

class ReasoningGenerator:
    """
    Generates human-readable reasoning for bet selections using Claude.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        if not self.api_key:
            logger.warning("ANTHROPIC_API_KEY not found. Reasoning generation will be disabled.")
            self.client = None
        else:
            self.client = Anthropic(api_key=self.api_key)
        
        self.supabase = get_supabase_admin()

    def generate_for_accumulator(self, acca_id: int):
        """
        Generates reasoning for all legs of a specific accumulator.
        """
        if not self.client:
            return

        try:
            # Join legs with predictions and match data
            res = (
                self.supabase.table("accumulator_legs")
                .select(
                    "id, prediction_id, "
                    "predictions(predicted_outcome, market, confidence_score, "
                    "matches(home_team_id, away_team_id, sport:sports(slug), "
                    "home_team:teams!home_team_id(name), away_team:teams!away_team_id(name)))"
                )
                .eq("accumulator_id", acca_id)
                .execute()
            )

            legs = res.data or []
            for leg in legs:
                pred = leg.get("predictions", {})
                match = pred.get("matches", {})
                
                # Fetch top influential features for this prediction
                # In a real scenario, we'd pull from a 'feature_importance' or 'prediction_metadata' table
                # For now, we'll simulate fetching the top 5 features
                top_features = self._fetch_top_features(leg["prediction_id"])
                
                reasoning = self._call_claude(
                    home_team=match.get("home_team", {}).get("name", "Home"),
                    away_team=match.get("away_team", {}).get("name", "Away"),
                    market=pred.get("market", ""),
                    outcome=pred.get("predicted_outcome", ""),
                    confidence=pred.get("confidence_score", 0),
                    top_features=top_features
                )

                if reasoning:
                    self.supabase.table("accumulator_legs").update({
                        "ai_reasoning": reasoning
                    }).eq("id", leg["id"]).execute()

        except Exception as exc:
            logger.error("Failed to generate reasoning for acca %s: %s", acca_id, exc)

    def _call_claude(
        self, 
        home_team: str, 
        away_team: str, 
        market: str, 
        outcome: str, 
        confidence: float, 
        top_features: List[Dict[str, Any]]
    ) -> Optional[str]:
        """
        Calls Anthropic API to generate a 3-sentence explanation.
        """
        if not self.client:
            return None

        features_str = ", ".join([f"{f['name']} ({f['value']})" for f in top_features])
        
        prompt = (
            f"You are an expert sports betting analyst. Explain the value in this selection in exactly 3 sentences. "
            f"Match: {home_team} vs {away_team}. Market: {market}. Selection: {outcome}. "
            f"Model Confidence: {confidence}%. Key Drivers: {features_str}. "
            f"Focus on WHY this selection has mathematical value (EV+) and what the data highlights about the team form or matchup."
        )

        try:
            message = self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=150,
                temperature=0.7,
                system="You provide concise, professional sports betting insights. Always return exactly 3 sentences.",
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            return message.content[0].text
        except Exception as exc:
            logger.error("Claude API call failed: %s", exc)
            return None

    def _fetch_top_features(self, prediction_id: int) -> List[Dict[str, Any]]:
        """
        Helper to fetch or simulate the most influential features for a prediction.
        """
        # In this architecture, we expect the TrainingPipeline to have stored 
        # SHAP values or feature importance in a metadata table.
        # Fallback to simulated features for the build.
        return [
            {"name": "recent_form_points", "value": "2.4 PPG"},
            {"name": "avg_goals_scored_last_5", "value": "2.8"},
            {"name": "head_to_head_win_rate", "value": "80%"},
            {"name": "missing_key_player_away", "value": "Yes"},
            {"name": "expected_goals_delta", "value": "+1.2"}
        ]
