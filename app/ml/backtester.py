"""
backtester.py — Performance simulator for Bet Hero ML models.

Simulates placing flat-stake bets on historical matches based on model 
predictions and closing odds. Validates if a model is "production-ready" 
by requiring a positive ROI over the backtest period.

Features
--------
1. Loads historical results + features + closing odds (Pinnacle preference).
2. Simulates 1 unit (flat stake) per bet where model_prob > implied_prob + edge_threshold.
3. Computes:
   - ROI (Return on Investment)
   - Win Rate
   - Max Drawdown (peak-to-trough decline)
   - Sharpe Ratio (risk-adjusted return)
   - Longest Win/Loss Streaks
4. Generates a matplotlib chart (PNG) of cumulative profit over time.

Usage
-----
    python -m app.ml.backtester --sport football --market match_result --years 2
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score

from ..database import get_supabase_admin
from .models import StackingEnsemble
from .training_pipeline import TrainingDataLoader, MARKETS, SUPPORTED_SPORTS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────── Data Classes ──────────────────────────────────

@dataclass
class BacktestMetric:
    sport:           str
    market:          str
    model_name:      str
    total_bets:      int
    won_bets:        int
    win_rate:        float
    roi:             float
    profit:          float
    max_drawdown:    float
    sharpe_ratio:    float
    win_streak:      int
    loss_streak:     int
    is_prod_ready:   bool
    chart_path:      str


# ─────────────────────────── Backtester Engine ─────────────────────────────

class Backtester:
    """
    Simulates betting history for a specific sport and market.
    """

    def __init__(self, sport: str, market: str, stake: float = 1.0):
        self.sport = sport
        self.market = market
        self.stake = stake
        self.client = get_supabase_admin()
        self.model = StackingEnsemble(sport=sport, market=market)

    def run(
        self,
        years: int = 2,
        edge_threshold: float = 0.02,
        min_prob: float = 0.45,
    ) -> BacktestMetric | None:
        """
        Runs the simulation.
        """
        logger.info("Starting backtest for %s/%s (last %d years)", self.sport, self.market, years)

        # 1. Load model
        try:
            self.model.load_base_models()
            # StackingEnsemble expects to be trained. If no ensemble model exists, we can't backtest it.
            # We'll try to load the saved meta-learner.
            ensemble_path = self.model.model_dir / f"{self.model.MODEL_NAME}_{self.sport}_{self.market}.joblib"
            if not ensemble_path.exists():
                logger.error("No trained ensemble model found at %s. Train it first.", ensemble_path)
                return None
            self.model.load(ensemble_path)
        except Exception as exc:
            logger.error("Failed to load model for backtest: %s", exc)
            return None

        # 2. Load data (Matches + Features + Targets)
        loader = TrainingDataLoader(self.client)
        df = loader.load(self.sport, self.market)
        if df is None or df.empty:
            logger.warning("No data found for backtest.")
            return None

        # Filter by date range
        cutoff = datetime.now(timezone.utc) - timedelta(days=years * 365)
        df = df[df["match_date"] >= cutoff].copy()
        if len(df) < 50:
            logger.warning("Insufficient data for backtest range (%d rows)", len(df))
            return None

        # 3. Load Odds for these matches
        odds_df = self._load_closing_odds(df["match_id"].tolist())
        if odds_df.empty:
            logger.error("No odds found for matches. Cannot compute ROI.")
            return None

        # Merge odds into main df
        df = df.merge(odds_df, on="match_id", how="inner")
        if df.empty:
            logger.error("Merge between matches and odds yielded empty set.")
            return None

        logger.info("Backtesting on %d matches with odds.", len(df))

        # 4. Generate Predictions
        # Identify feature columns (match training_pipeline approach)
        EXCLUDE = {"match_id", "match_date", "sport_id", "league_id",
                   "home_score", "away_score", "home_yellow_cards", "away_yellow_cards",
                   "home_corners", "away_corners", "target", "status", "season",
                   "closing_home", "closing_draw", "closing_away", "bookmaker"}
        feat_cols = [c for c in df.columns if c in self.model._feature_names or 
                     (c not in EXCLUDE and pd.api.types.is_numeric_dtype(df[c]))]
        
        X = df[feat_cols].fillna(0.0)
        batch = self.model.predict_proba(X)
        
        # 5. Simulate Bets
        history = []
        cumulative_pnl = 0.0
        pnl_series = [0.0]
        
        # Mapping market outcomes to odds columns
        # Index: H=0/Home=0, D=1/Draw=1, A=2/Away=2 (standardized to classes)
        classes = self.model._classes
        
        for i, (idx, row) in enumerate(df.iterrows()):
            probs = batch.probabilities.iloc[i]
            target = row["target"]
            
            # Find best value bet (max positive edge)
            best_outcome = None
            max_edge = -1.0
            best_odds = 0.0
            
            for outcome in classes:
                # Get implied prob from odds
                o_val = self._get_odds_for_outcome(row, outcome)
                if o_val <= 1.0: continue
                
                implied = 1.0 / o_val
                m_prob = probs[outcome]
                edge = m_prob - implied
                
                if edge > max_edge:
                    max_edge = edge
                    best_outcome = outcome
                    best_odds = o_val
            
            # Decision rule: positive edge + confidence threshold
            if best_outcome and max_edge >= edge_threshold and probs[best_outcome] >= min_prob:
                is_win = (best_outcome == target)
                profit = (self.stake * (best_odds - 1)) if is_win else -self.stake
                cumulative_pnl += profit
                
                history.append({
                    "date": row["match_date"],
                    "outcome": best_outcome,
                    "target": target,
                    "odds": best_odds,
                    "edge": max_edge,
                    "prob": probs[best_outcome],
                    "profit": profit,
                    "is_win": is_win
                })
            else:
                # No bet placed
                pass
            
            pnl_series.append(cumulative_pnl)

        if not history:
            logger.warning("No bets met the threshold criteria.")
            return None

        # 6. Compute Metrics
        results_df = pd.DataFrame(history)
        total_bets = len(results_df)
        won_bets = results_df["is_win"].sum()
        win_rate = won_bets / total_bets if total_bets > 0 else 0
        roi = results_df["profit"].sum() / (total_bets * self.stake) if total_bets > 0 else 0
        total_profit = results_df["profit"].sum()

        # Streaks
        wins = results_df["is_win"].values
        win_streak = self._get_max_streak(wins, True)
        loss_streak = self._get_max_streak(wins, False)

        # Drawdown
        cum_pnl = np.array(pnl_series)
        running_max = np.maximum.accumulate(cum_pnl)
        drawdown = running_max - cum_pnl
        max_drawdown = np.max(drawdown)

        # Sharpe (Daily approximation if multiple bets per day, or per-bet)
        daily_returns = results_df.groupby("date")["profit"].sum()
        if len(daily_returns) > 1:
            sharpe = (daily_returns.mean() / daily_returns.std() * np.sqrt(365)) if daily_returns.std() > 0 else 0
        else:
            sharpe = 0.0

        # Chart
        chart_path = self._generate_chart(results_df)

        metric = BacktestMetric(
            sport=self.sport,
            market=self.market,
            model_name=self.model.MODEL_NAME,
            total_bets=total_bets,
            won_bets=won_bets,
            win_rate=round(win_rate, 4),
            roi=round(roi, 4),
            profit=round(total_profit, 2),
            max_drawdown=round(max_drawdown, 2),
            sharpe_ratio=round(sharpe, 2),
            win_streak=win_streak,
            loss_streak=loss_streak,
            is_prod_ready=(roi > 0),
            chart_path=chart_path
        )

        self._print_report(metric)
        return metric

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _load_closing_odds(self, match_ids: list[int]) -> pd.DataFrame:
        """Loads Pinnacle closing odds from odds_history."""
        try:
            # We want one row per match, preferring Pinnacle
            res = (
                self.client.table("odds_history")
                .select("match_id, bookmaker, closing_home, closing_draw, closing_away")
                .in_("match_id", match_ids)
                .execute()
            )
            if not res.data:
                return pd.DataFrame()
            
            odds_df = pd.DataFrame(res.data)
            # Prefer Pinnacle
            is_pinnacle = odds_df["bookmaker"].str.lower().str.contains("pinnacle", na=False)
            pinnacle_df = odds_df[is_pinnacle].drop_duplicates("match_id")
            
            # Fallback for matches where Pinnacle is missing
            other_df = odds_df[~is_pinnacle].drop_duplicates("match_id")
            final_df = pd.concat([pinnacle_df, other_df[~other_df["match_id"].isin(pinnacle_df["match_id"])]])
            
            return final_df
        except Exception as exc:
            logger.error("Error loading odds: %s", exc)
            return pd.DataFrame()

    def _get_odds_for_outcome(self, row: pd.Series, outcome: str) -> float:
        """Maps H/D/A or Yes/No etc to the correct odds column."""
        if outcome in ["H", "Home", "Yes", "Over"]:
            return row.get("closing_home", 0.0)
        elif outcome in ["D", "Draw"]:
            return row.get("closing_draw", 0.0)
        elif outcome in ["A", "Away", "No", "Under"]:
            return row.get("closing_away", 0.0)
        return 0.0

    def _get_max_streak(self, series: np.ndarray, target: bool) -> int:
        max_streak = 0
        current_streak = 0
        for val in series:
            if val == target:
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0
        return max_streak

    def _generate_chart(self, results_df: pd.DataFrame) -> str:
        """Generates a PNG performance chart."""
        plt.figure(figsize=(10, 6))
        
        # Sort by date for chronologic plot
        df_sorted = results_df.sort_values("date").copy()
        df_sorted["cum_profit"] = df_sorted["profit"].cumsum()
        
        plt.plot(df_sorted["date"], df_sorted["cum_profit"], color="#00ff00", linewidth=1.5, label="Cumulative P/L")
        plt.axhline(0, color="red", linestyle="--", alpha=0.5)
        
        plt.title(f"Backtest Performance: {self.sport} - {self.market}", fontsize=14, pad=15)
        plt.xlabel("Date")
        plt.ylabel("Profit (Units)")
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        plt.tight_layout()
        
        output_dir = Path("reports/backtests")
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{self.sport}_{self.market}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        path = output_dir / filename
        plt.savefig(path)
        plt.close()
        
        return str(path)

    def _print_report(self, m: BacktestMetric):
        """Prints a professional backtest summary."""
        status = "PROD READY" if m.is_prod_ready else "REJECTED"
        color = "\033[92m" if m.is_prod_ready else "\033[91m"
        reset = "\033[0m"
        
        print(f"\n{color}═══ BACKTEST REPORT: {m.sport} | {m.market} | {status} ═══{reset}")
        print(f" Model:          {m.model_name}")
        print(f" Total Bets:     {m.total_bets}")
        print(f" Win Rate:       {m.win_rate:.2%}")
        print(f" ROI:            {m.roi:+.2%}")
        print(f" Total Profit:   {m.profit:+.2f} Units")
        print(f" Max Drawdown:   {m.max_drawdown:.2f} Units")
        print(f" Sharpe Ratio:   {m.sharpe_ratio:.2f}")
        print(f" Longest Win:    {m.win_streak} games")
        print(f" Longest Loss:   {m.loss_streak} games")
        print(f" Chart saved to: {m.chart_path}")
        print(f"{color}{'═'*65}{reset}\n")

# ─────────────────────────── CLI Entry Point ───────────────────────────────

def _cli():
    ap = argparse.ArgumentParser(description="Bet Hero Backtester")
    ap.add_argument("--sport",   required=True, help="Sport slug")
    ap.add_argument("--market",  required=True, help="Market slug")
    ap.add_argument("--years",   type=int, default=2, help="Years to simulate")
    ap.add_argument("--edge",    type=float, default=0.03, help="Edge threshold (e.g. 0.03 for 3%)")
    ap.add_argument("--min_prob",type=float, default=0.45, help="Min model probability to bet")
    ap.add_argument("--stake",   type=float, default=1.0, help="Flat stake amount")
    args = ap.parse_args()

    # Verify market and sport
    if args.sport not in SUPPORTED_SPORTS and args.sport != "all":
        logger.warning("Unrecognized sport: %s", args.sport)
    
    if args.market not in MARKETS:
        logger.warning("Unrecognized market: %s", args.market)

    backtester = Backtester(args.sport, args.market, stake=args.stake)
    backtester.run(years=args.years, edge_threshold=args.edge, min_prob=args.min_prob)

if __name__ == "__main__":
    _cli()
