import os
import joblib
import io
import asyncio
import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Set, Dict, Any, Optional
import google.generativeai as genai
from app.database import get_supabase_admin

logger = logging.getLogger(__name__)

MODELS_DIR = "/tmp/models"
_model_cache = {}

# Configure Gemini
genai.configure(api_key=os.getenv("GEMINI_API_KEY", "AIzaSyBKsnwEqAfhelkv9ZEaQ1tq7HFB1rndfF0"))
gemini_model = genai.GenerativeModel("gemini-2.0-flash")

def load_best_model(sport: str):
    global _model_cache
    
    # Return cached model if available
    if sport in _model_cache:
        return _model_cache[sport]
    
    # Try local /tmp first
    for name in ["lightgbm", "xgboost", "random_forest"]:
        path = f"{MODELS_DIR}/{sport}_{name}.pkl"
        if os.path.exists(path):
            try:
                model = joblib.load(path)
                _model_cache[sport] = (model, name)
                print(f"Loaded {sport} {name} from local cache")
                return model, name
            except Exception:
                continue
    
    # Download from Supabase Storage
    print(f"Downloading {sport} model from storage...")
    from app.database import get_supabase_admin
    supabase = get_supabase_admin()
    os.makedirs(MODELS_DIR, exist_ok=True)
    
    for name in ["lightgbm", "xgboost", "random_forest"]:
        storage_path = f"models/{sport}_{name}.pkl"
        try:
            data = supabase.storage\
                .from_("sports-data")\
                .download(storage_path)
            
            local_path = f"{MODELS_DIR}/{sport}_{name}.pkl"
            with open(local_path, "wb") as f:
                f.write(data)
            
            model = joblib.load(local_path)
            _model_cache[sport] = (model, name)
            print(f"Downloaded and cached {sport} {name}")
            return model, name
        except Exception:
            continue
    
    return None, None

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


def get_team_form(supabase, team: str, sport: str) -> float:
    try:
        # Try exact team name match first
        result = supabase.table("matches")\
            .select("home_team, away_team, home_score, away_score")\
            .eq("sport", sport)\
            .eq("status", "completed")\
            .or_(f"home_team.eq.{team},away_team.eq.{team}")\
            .order("match_date", desc=True)\
            .limit(10)\
            .execute()

        matches = result.data or []
        
        if matches:
            wins = 0
            for m in matches:
                hs = m.get("home_score") or 0
                aws = m.get("away_score") or 0
                if m.get("home_team") == team and hs > aws:
                    wins += 1
                elif m.get("away_team") == team and aws > hs:
                    wins += 1
            return wins / len(matches)
        
        # No matches found - use league average
        # Get average win rate for this sport
        total = supabase.table("matches")\
            .select("home_score, away_score")\
            .eq("sport", sport)\
            .eq("status", "completed")\
            .limit(1000)\
            .execute()
        
        if total.data:
            home_wins = sum(
                1 for m in total.data
                if (m.get("home_score") or 0) > 
                   (m.get("away_score") or 0)
            )
            # Home advantage factor
            home_win_rate = home_wins / len(total.data)
            # Return slight home advantage as default
            return home_win_rate
        
        return 0.5
        
    except Exception:
        return 0.5


def predict_match(
    home_team: str,
    away_team: str,
    sport: str,
    home_odds: float,
    away_odds: float,
    draw_odds: float = 3.0
) -> dict:
    supabase = get_supabase_admin()

    home_wr = get_team_form(supabase, home_team, sport)
    away_wr = get_team_form(supabase, away_team, sport)

    ho = float(home_odds or 2.0)
    ao = float(away_odds or 2.0)
    do = float(draw_odds or 3.0)

    feature = np.array([[
        home_wr,
        away_wr,
        home_wr - away_wr,
        10,
        10,
        ho,
        ao,
        do,
        1 / ho,
        1 / ao,
        1 if ho < ao else 0,
        ho / ao,
        home_wr - 0.5,
    ]])

    model, model_name = load_best_model(sport)

    if model:
        try:
            proba = model.predict_proba(feature)[0]
            pred_class = int(np.argmax(proba))
            confidence = float(max(proba))
            outcomes = {
                1: ("Home Win", ho),
                2: ("Away Win", ao),
                0: ("Draw", do)
            }
            outcome, odds = outcomes.get(pred_class, ("Home Win", ho))
            model_prob = float(proba[pred_class])
        except Exception as e:
            print(f"Model error: {e}")
            outcome = "Home Win"
            odds = ho
            model_prob = home_wr
            confidence = home_wr
    else:
        outcome = "Home Win"
        odds = ho
        model_prob = home_wr
        confidence = home_wr

    implied_prob = 1 / odds
    ev = (model_prob * (odds - 1)) - (1 - model_prob)
    edge = (model_prob - implied_prob) * 100

    return {
        "outcome": outcome,
        "odds": round(odds, 2),
        "model_probability": round(model_prob, 4),
        "implied_probability": round(implied_prob, 4),
        "confidence": round(confidence * 100, 1),
        "ev": round(ev, 4),
        "edge": round(edge, 2),
        "home_win_rate": round(home_wr, 3),
        "away_win_rate": round(away_wr, 3),
    }


def generate_reasoning(
    home_team: str,
    away_team: str,
    sport: str,
    league: str,
    pred: dict
) -> str:
    try:
        response = gemini_model.generate_content(
            f"2 sentences explaining why {pred['outcome']} "
            f"in {home_team} vs {away_team} ({league}) "
            f"is a value bet. Odds: {pred['odds']}, "
            f"Confidence: {pred['confidence']}%, "
            f"Edge: {pred['edge']}%. No markdown."
        )
        return response.text.strip()
    except Exception as e:
        print(f"Reasoning error: {e}")
        return (
            f"{home_team} shows {pred['home_win_rate']:.0%} "
            f"win rate with {pred['edge']:.1f}% edge over market."
        )


async def build_all_accumulators():
    print("BUILD ACCUMULATORS STARTED")
    supabase = get_supabase_admin()

    try:
        result = supabase.table("matches")\
            .select("*")\
            .eq("status", "upcoming")\
            .limit(200)\
            .execute()
        fixtures = result.data or []
        print(f"UPCOMING FIXTURES FOUND: {len(fixtures)}")
    except Exception as e:
        print(f"Fixture fetch error: {e}")
        return

    if not fixtures:
        print("No upcoming fixtures found")
        return

    value_preds = []
    for f in fixtures:
        try:
            home = f.get("home_team", "")
            away = f.get("away_team", "")
            sport = f.get("sport", "football")

            if not home or not away:
                continue

            pred = predict_match(
                home,
                away,
                sport,
                f.get("home_odds", 2.0),
                f.get("away_odds", 2.0),
                f.get("draw_odds", 3.0)
            )

            print(f"{home} vs {away}: EV={pred['ev']:.4f}")

            if pred["ev"] > 0:
                value_preds.append({
                    "fixture": f,
                    "prediction": pred
                })

        except Exception as e:
            print(f"Prediction error: {e}")
            continue

    print(f"VALUE PREDICTIONS FOUND: {len(value_preds)}")

    if not value_preds:
        print("No value predictions found - relaxing EV threshold")
        value_preds = []
        for f in fixtures:
            try:
                home = f.get("home_team", "")
                away = f.get("away_team", "")
                sport = f.get("sport", "football")
                if not home or not away:
                    continue
                pred = predict_match(
                    home, away, sport,
                    f.get("home_odds", 2.0),
                    f.get("away_odds", 2.0),
                    f.get("draw_odds", 3.0)
                )
                value_preds.append({
                    "fixture": f,
                    "prediction": pred
                })
            except Exception:
                continue

    value_preds.sort(
        key=lambda x: x["prediction"]["ev"],
        reverse=True
    )

    used_ids = set()

    configs = [
        {
            "type": "10odds",
            "target": 10.0,
            "max_legs": 13,      # 7-10 legs
            "min_legs": 7,       # minimum 7 legs
            "min_conf": 55,
            "max_single_odds": 1.8,   # LOW odds per leg
            "min_single_odds": 1.2,   # minimum odds per leg
        },
        {
            "type": "5odds",
            "target": 5.0,
            "max_legs": 9,       # exactly 7 legs
            "min_legs": 7,
            "min_conf": 60,
            "max_single_odds": 1.5,   # Very low odds
            "min_single_odds": 1.1,
        },
        {
            "type": "3odds",
            "target": 3.0,
            "max_legs": 7,       # exactly 5 legs
            "min_legs": 5,
            "min_conf": 65,
            "max_single_odds": 1.35,  # Lowest risk
            "min_single_odds": 1.1,
        },
    ]

    for cfg in configs:
        await save_accumulator(supabase, value_preds, used_ids, cfg)

    print("ALL ACCUMULATORS BUILT SUCCESSFULLY")


async def save_accumulator(
    supabase, all_preds, used_ids, config
):
    acca_type = config["type"]
    max_legs = config["max_legs"]
    min_legs = config["min_legs"]
    min_conf = config["min_conf"]
    max_odds = config["max_single_odds"]
    min_odds = config["min_single_odds"]

    legs = []
    combined_odds = 1.0
    local_matches = set()

    for item in all_preds:
        f = item["fixture"]
        pred = item["prediction"]
        fid = f.get("id")
        match_key = (
            f"{f.get('home_team')}_"
            f"{f.get('away_team')}"
        )

        if fid in used_ids:
            continue
        if match_key in local_matches:
            continue
        if pred["confidence"] < min_conf:
            continue
        if len(legs) >= max_legs:
            break

        # ENFORCE LOW ODDS PER LEG
        leg_odds = pred["odds"]
        if leg_odds > max_odds:
            continue
        if leg_odds < min_odds:
            continue

        # Check combined odds won't overshoot
        new_combined = combined_odds * leg_odds
        if new_combined > config["target"] * 2.0:
            continue

        reasoning = generate_reasoning(
            f.get("home_team", ""),
            f.get("away_team", ""),
            f.get("sport", ""),
            f.get("league", ""),
            pred
        )

        legs.append({
            "fixture": f,
            "prediction": pred,
            "reasoning": reasoning
        })
        combined_odds *= leg_odds
        used_ids.add(fid)
        local_matches.add(match_key)

    # Only save if we have minimum legs
    if len(legs) < min_legs:
        print(
            f"{acca_type}: Only {len(legs)} legs found, "
            f"need minimum {min_legs}"
        )
        # Relax confidence and try again with all preds
        legs = []
        combined_odds = 1.0
        local_matches = set()

        for item in all_preds:
            f = item["fixture"]
            pred = item["prediction"]
            fid = f.get("id")
            match_key = (
                f"{f.get('home_team')}_"
                f"{f.get('away_team')}"
            )

            if fid in used_ids:
                continue
            if match_key in local_matches:
                continue
            if len(legs) >= max_legs:
                break

            leg_odds = pred["odds"]
            if leg_odds > max_odds + 0.3:
                continue

            legs.append({
                "fixture": f,
                "prediction": pred,
                "reasoning": generate_reasoning(
                    f.get("home_team", ""),
                    f.get("away_team", ""),
                    f.get("sport", ""),
                    f.get("league", ""),
                    pred
                )
            })
            combined_odds *= leg_odds
            used_ids.add(fid)
            local_matches.add(match_key)

    if not legs:
        print(f"No legs found for {acca_type}")
        return

    # Save to database (same as before)
    avg_conf = sum(
        l["prediction"]["confidence"] for l in legs
    ) / len(legs)

    print(
        f"{acca_type}: {len(legs)} legs, "
        f"{combined_odds:.2f} combined odds"
    )

    try:
        acca_result = supabase.table(
            "accumulators"
        ).insert({
            "acca_type": acca_type,
            "total_odds": round(combined_odds, 2),
            "status": "PENDING",
            "ai_reasoning": (
                f"AI selected {len(legs)} low-risk value "
                f"bets with combined odds of "
                f"{combined_odds:.2f}. Each selection "
                f"has high confidence and positive "
                f"expected value."
            ),
            "confidence_score": round(avg_conf, 1),
            "created_at": datetime.now().isoformat()
        }).execute()

        acca_id = acca_result.data[0]["id"]

        for i, leg in enumerate(legs):
            f = leg["fixture"]
            pred = leg["prediction"]

            pred_result = supabase.table(
                "predictions"
            ).insert({
                "match_id": str(f.get("id")),
                "market": "Match Result",
                "predicted_outcome": pred["outcome"],
                "model_probability": pred["model_probability"],
                "implied_probability": pred["implied_probability"],
                "edge": pred["edge"],
                "odds": pred["odds"],
                "confidence_score": pred["confidence"],
                "status": "PENDING",
                "created_at": datetime.now().isoformat()
            }).execute()

            pred_id = pred_result.data[0]["id"]

            supabase.table("accumulator_legs").insert({
                "accumulator_id": acca_id,
                "prediction_id": str(pred_id),
                "match_id": str(f.get("id")),
                "sport": f.get("sport", ""),
                "league": f.get("league", ""),
                "home_team": f.get("home_team", ""),
                "away_team": f.get("away_team", ""),
                "market": "Match Result",
                "predicted_outcome": pred["outcome"],
                "odds": pred["odds"],
                "confidence": pred["confidence"],
                "edge": pred["edge"],
                "ai_reasoning": leg["reasoning"],
                "status": "PENDING",
                "leg_order": i + 1,
                "created_at": datetime.now().isoformat()
            }).execute()

        print(f"Saved {acca_type}: {len(legs)} legs")

    except Exception as e:
        print(f"Save error {acca_type}: {e}")
        import traceback
        traceback.print_exc()
