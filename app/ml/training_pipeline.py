import os
import pandas as pd
import numpy as np
from datetime import datetime
from app.database import get_supabase_admin

try:
    from sklearn.ensemble import (
        RandomForestClassifier, GradientBoostingClassifier
    )
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import LabelEncoder
    from sklearn.metrics import accuracy_score
    import xgboost as xgb
    import lightgbm as lgb
    import joblib
    MODELS_AVAILABLE = True
except ImportError as e:
    print(f"ML imports: {e}")
    MODELS_AVAILABLE = False

MODELS_DIR = "/tmp/models"

def save_model_to_storage(
    model, 
    sport: str, 
    model_name: str
):
    import joblib
    import io
    from app.database import get_supabase_admin
    
    supabase = get_supabase_admin()
    
    # Save to bytes
    buffer = io.BytesIO()
    joblib.dump(model, buffer)
    buffer.seek(0)
    
    storage_path = f"models/{sport}_{model_name}.pkl"
    
    try:
        supabase.storage.from_("sports-data").upload(
            path=storage_path,
            file=buffer.read(),
            file_options={
                "content-type": "application/octet-stream",
                "upsert": "true"
            }
        )
        print(f"Model saved to storage: {storage_path}")
    except Exception as e:
        print(f"Model storage error: {e}")

def get_training_data(sport: str) -> pd.DataFrame:
    supabase = get_supabase_admin()
    print(f"Fetching {sport} training data...")
    
    all_data = []
    page_size = 10000
    offset = 0
    max_records = 200000  # Cap at 200K to avoid memory issues
    
    while True:
        try:
            result = supabase.table("matches")\
                .select(
                    "home_team, away_team, sport, "
                    "home_score, away_score, "
                    "home_odds, away_odds, draw_odds, "
                    "match_date, result"
                )\
                .eq("sport", sport)\
                .eq("status", "completed")\
                .not_.is_("home_score", "null")\
                .not_.is_("away_score", "null")\
                .order("match_date")\
                .range(offset, offset + page_size - 1)\
                .execute()
            
            batch = result.data or []
            
            if not batch:
                print(f"No more data at offset {offset}")
                break
            
            all_data.extend(batch)
            print(f"Fetched {len(all_data)} {sport} records...")
            
            if len(batch) < page_size:
                break
            
            if len(all_data) >= max_records:
                print(f"Reached max cap: {max_records}")
                break
                
            offset += page_size
            
        except Exception as e:
            print(f"Fetch error at offset {offset}: {e}")
            break
    
    print(f"Total {sport} training records: {len(all_data)}")
    
    if not all_data:
        return pd.DataFrame()
    
    return pd.DataFrame(all_data)

def engineer_features(df: pd.DataFrame, sport: str):
    """Create features for ML model"""
    features = []
    labels = []
    
    # Sort by date
    if "match_date" in df.columns:
        df = df.sort_values("match_date")
    
    # Build team form lookup
    team_wins = {}
    team_games = {}
    
    for _, row in df.iterrows():
        home = str(row.get("home_team", ""))
        away = str(row.get("away_team", ""))
        home_score = row.get("home_score", 0) or 0
        away_score = row.get("away_score", 0) or 0
        
        if not home or not away:
            continue
        
        # Get current form
        home_wr = team_wins.get(home, 0) / max(
            team_games.get(home, 1), 1
        )
        away_wr = team_wins.get(away, 0) / max(
            team_games.get(away, 1), 1
        )
        
        h_odds = float(row.get("home_odds", 2.0) or 2.0)
        a_odds = float(row.get("away_odds", 2.0) or 2.0)
        d_odds = float(row.get("draw_odds", 3.0) or 3.0)
        
        feature = [
            home_wr,
            away_wr,
            home_wr - away_wr,
            team_games.get(home, 0),
            team_games.get(away, 0),
            h_odds,
            a_odds,
            d_odds,
            1 / h_odds,   # NEW: implied_home_prob
            1 / a_odds,   # NEW: implied_away_prob
            1 if h_odds < a_odds else 0, # NEW: market_favorite
            h_odds / a_odds,  # NEW: odds_ratio
            home_wr - 0.5,    # NEW: form_momentum
        ]
        
        # Determine result
        if home_score > away_score:
            result = 1  # Home win
        elif home_score < away_score:
            result = 2  # Away win
        else:
            result = 0  # Draw
        
        features.append(feature)
        labels.append(result)
        
        # Update form
        for team in [home, away]:
            team_games[team] = team_games.get(team, 0) + 1
        
        if home_score > away_score:
            team_wins[home] = team_wins.get(home, 0) + 1
        elif away_score > home_score:
            team_wins[away] = team_wins.get(away, 0) + 1
    
    return features, labels

async def train_sport_models(sport: str):
    """Train ML models for a specific sport"""
    print(f"\n{'='*50}")
    print(f"TRAINING MODELS FOR: {sport.upper()}")
    print(f"{'='*50}")
    
    if not MODELS_AVAILABLE:
        print("ML libraries not available")
        return {"error": "ML libraries not available"}
    
    # Get data
    df = get_training_data(sport)
    
    if df.empty or len(df) < 100:
        print(f"Insufficient data for {sport}: {len(df)} records")
        return {"error": f"Insufficient data: {len(df)} records"}
    
    print(f"Engineering features from {len(df)} matches...")
    features, labels = engineer_features(df, sport)
    
    if len(features) < 100:
        print(f"Insufficient features: {len(features)}")
        return {"error": "Insufficient features"}
    
    X = np.array(features)
    y = np.array(labels)
    
    # Time-based split (no data leakage)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    
    print(f"Training: {len(X_train)} | Test: {len(X_test)}")
    
    os.makedirs(MODELS_DIR, exist_ok=True)
    results = {}
    
    models_to_train = [
        ("random_forest", RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1
        )),
        ("xgboost", xgb.XGBClassifier(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
            eval_metric="mlogloss",
            verbosity=0
        )),
        ("lightgbm", lgb.LGBMClassifier(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
            verbose=-1
        )),
    ]
    
    for model_name, model in models_to_train:
        try:
            print(f"Training {model_name}...")
            model.fit(X_train, y_train)
            
            # Persistent Storage: Upload to Supabase
            save_model_to_storage(model, sport, model_name)
            
            y_pred = model.predict(X_test)
            accuracy = accuracy_score(y_test, y_pred)
            
            print(f"{model_name} accuracy: {accuracy:.4f}")
            
            # Save model
            model_path = f"{MODELS_DIR}/{sport}_{model_name}.pkl"
            joblib.dump(model, model_path)
            
            # Save to database
            supabase = get_supabase_admin()
            supabase.table("model_performance").upsert({
                "model_name": model_name,
                "sport": sport,
                "market": "match_result",
                "accuracy": float(accuracy),
                "roi": float((accuracy - 0.5) * 20),
                "win_rate": float(accuracy),
                "total_predictions": len(X_test),
                "recorded_at": datetime.now().isoformat()
            }).execute()
            
            results[model_name] = {
                "accuracy": round(accuracy, 4),
                "status": "trained"
            }
            
        except Exception as e:
            print(f"{model_name} training error: {e}")
            results[model_name] = {"error": str(e)}
    
    print(f"\nTraining complete for {sport}:")
    for model, result in results.items():
        print(f"  {model}: {result}")
    
    return {
        "sport": sport,
        "records_used": len(df),
        "models": results,
        "completed_at": datetime.now().isoformat()
    }

async def train_all_models():
    """Train models for all sports"""
    sports = [
        "football", "tennis", "basketball",
        "nfl", "cricket", "nhl", "mlb"
    ]
    results = {}
    for sport in sports:
        try:
            result = await train_sport_models(sport)
            results[sport] = result
        except Exception as e:
            results[sport] = {"error": str(e)}
    return results
