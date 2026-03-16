import pandas as pd
import numpy as np
from datetime import datetime

def run_backtest(
    model,
    X_test: pd.DataFrame,
    y_test: pd.Series,
    odds_test: pd.Series = None,
    sport: str = "football"
) -> dict:
    """
    Run backtest simulation on test data.
    Returns ROI, accuracy, win rate stats.
    """
    try:
        predictions = model.predict(X_test)
        probabilities = model.predict_proba(X_test)
        
        correct = (predictions == y_test).sum()
        total = len(y_test)
        accuracy = correct / total if total > 0 else 0
        
        # Simulate flat stake betting
        stake = 10.0
        total_staked = 0
        total_returned = 0
        wins = 0
        losses = 0
        
        if odds_test is not None:
            for i, (pred, actual, odds) in enumerate(
                zip(predictions, y_test, odds_test)
            ):
                total_staked += stake
                if pred == actual:
                    total_returned += stake * float(odds)
                    wins += 1
                else:
                    losses += 1
            
            roi = (
                (total_returned - total_staked) 
                / total_staked * 100
            ) if total_staked > 0 else 0
        else:
            # No odds available - use accuracy-based estimate
            roi = (accuracy - 0.5) * 20
            wins = correct
            losses = total - correct
        
        return {
            "accuracy": round(accuracy, 4),
            "roi": round(roi, 2),
            "win_rate": round(wins/total, 4) if total > 0 else 0,
            "total_predictions": total,
            "wins": wins,
            "losses": losses,
            "sport": sport,
            "recorded_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        print(f"Backtest error: {e}")
        return {
            "accuracy": 0,
            "roi": 0,
            "win_rate": 0,
            "total_predictions": 0,
            "wins": 0,
            "losses": 0,
            "sport": sport,
            "error": str(e)
        }

def generate_performance_report(
    results: dict,
    model_name: str,
    sport: str
) -> dict:
    """Generate a performance summary report"""
    return {
        "model_name": model_name,
        "sport": sport,
        "accuracy": results.get("accuracy", 0),
        "roi": results.get("roi", 0),
        "win_rate": results.get("win_rate", 0),
        "total_predictions": results.get(
            "total_predictions", 0
        ),
        "grade": grade_model(results.get("accuracy", 0)),
        "recorded_at": datetime.now().isoformat()
    }

def grade_model(accuracy: float) -> str:
    """Grade model performance"""
    if accuracy >= 0.70:
        return "EXCELLENT"
    elif accuracy >= 0.60:
        return "GOOD"
    elif accuracy >= 0.55:
        return "ACCEPTABLE"
    elif accuracy >= 0.50:
        return "WEAK"
    else:
        return "POOR"
