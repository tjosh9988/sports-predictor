import os
import io
import gc
import psutil
import pandas as pd
from datetime import datetime
from app.database import get_supabase_admin

BUCKET = "sports-data"

FOLDER_MAP = {
    "football": "football",
    "tennis": "tennis_atp",
    "tennis_atp": "tennis_atp",
    "tennis_wta": "tennis_wta",
    "nba": "nba",
    "nfl": "nfl",
    "cricket": "cricket",
    "nhl": "nhl",
    "mlb": "mlb",
    "basketball": "nba",
}

def insert_matches_batch(
    matches: list, 
    sport: str
):
    """Insert matches in batches of 500"""
    supabase = get_supabase_admin()
    batch_size = 500
    inserted = 0
    
    for i in range(0, len(matches), batch_size):
        batch = matches[i:i + batch_size]
        try:
            supabase.table("matches")\
                .upsert(batch)\
                .execute()
            inserted += len(batch)
            print(f"{sport}: inserted {inserted} records")
        except Exception as e:
            print(f"Batch insert error: {e}")
    
    return inserted

def _safe_int(val) -> int:
    try:
        if pd.isna(val):
            return None
        return int(float(val))
    except:
        return None

def _safe_float(val) -> float:
    try:
        if pd.isna(val):
            return None
        return float(val)
    except:
        return None

def check_memory():
    """Check if we have enough memory to continue"""
    mem = psutil.virtual_memory()
    available_mb = mem.available / 1024 / 1024
    if available_mb < 100:
        print(f"Low memory: {available_mb:.0f}MB available")
        return False
    return True

def map_row_football(row):
    home = str(row.get("HomeTeam",
               row.get("home_team",
               row.get("Home",""))))
    away = str(row.get("AwayTeam", 
               row.get("away_team",
               row.get("Away",""))))
    
    if not home or not away or home == "nan":
        return None
    
    return {
        "sport": "football",
        "league": str(row.get("Div",
                      row.get("League",
                      row.get("league","Unknown")))),
        "home_team": home,
        "away_team": away,
        "match_date": str(row.get("Date",
                          row.get("date",
                          "2000-01-01"))),
        "status": "completed",
        "home_score": _safe_int(row.get("FTHG", row.get("HG"))),
        "away_score": _safe_int(row.get("FTAG", row.get("AG"))),
        "result": str(row.get("FTR", row.get("Res",""))),
        "home_shots": _safe_int(row.get("HS")),
        "away_shots": _safe_int(row.get("AS")),
        "home_corners": _safe_int(row.get("HC")),
        "away_corners": _safe_int(row.get("AC")),
        "home_yellow_cards": _safe_int(row.get("HY")),
        "away_yellow_cards": _safe_int(row.get("AY")),
        "home_odds": _safe_float(row.get("B365H",row.get("BbAvH"))),
        "draw_odds": _safe_float(row.get("B365D",row.get("BbAvD"))),
        "away_odds": _safe_float(row.get("B365A",row.get("BbAvA"))),
    }

def map_row_tennis(row):
    winner = str(row.get("winner_name", row.get("player1","")))
    loser = str(row.get("loser_name", row.get("player2","")))
    if not winner or winner == "nan":
        return None
    return {
        "sport": "tennis",
        "league": str(row.get("tourney_name", row.get("tournament", "Unknown"))),
        "home_team": winner,
        "away_team": loser,
        "match_date": str(row.get("tourney_date", row.get("date", "2000-01-01"))),
        "status": "completed",
        "result": "H",
        "venue": str(row.get("surface","")),
    }

def map_row_nba(row):
    home = str(row.get("home_team", row.get("TEAM_NAME", row.get("team_name_home",""))))
    away = str(row.get("visitor_team", row.get("team_name_away", row.get("MATCHUP",""))))
    if not home or home == "nan":
        return None
    return {
        "sport": "basketball",
        "league": "NBA",
        "home_team": home,
        "away_team": away,
        "match_date": str(row.get("game_date", row.get("GAME_DATE", "2000-01-01"))),
        "status": "completed",
        "home_score": _safe_int(row.get("home_team_score", row.get("PTS"))),
        "away_score": _safe_int(row.get("visitor_team_score")),
    }

def process_df_chunked(file_bytes, sport, map_func):
    """Process CSV in memory-efficient chunks"""
    total = 0
    chunk_size = 1000
    current_matches = []
    
    try:
        reader = pd.read_csv(
            io.BytesIO(file_bytes),
            low_memory=False,
            chunksize=chunk_size
        )
        
        for chunk in reader:
            if not check_memory():
                print(f"Aborting {sport} import due to low memory")
                break
                
            for _, row in chunk.iterrows():
                try:
                    match = map_func(row)
                    if match:
                        current_matches.append(match)
                except:
                    continue
            
            if len(current_matches) >= chunk_size:
                total += insert_matches_batch(current_matches, sport)
                current_matches = []
                gc.collect() # Proactive cleanup
                
    except Exception as e:
        print(f"Chunk processing error: {e}")
        
    if current_matches:
        total += insert_matches_batch(current_matches, sport)
        
    gc.collect()
    return total

async def run_single_importer(sport: str):
    import io
    import pandas as pd
    from app.database import get_supabase_admin
    
    supabase = get_supabase_admin()
    print(f"Starting import for: {sport}")
    
    folder = FOLDER_MAP.get(sport, sport)
    
    # List files
    try:
        files = supabase.storage\
            .from_(BUCKET)\
            .list(folder)
        
        if not files:
            print(f"No files in storage/{folder}")
            return 0
            
        data_files = [
            f for f in files 
            if f.get("name","").endswith(('.csv','.json'))
            and not f.get("name","").startswith('.')
        ]
        print(f"Found {len(data_files)} files for {sport} in folder {folder}")
        
    except Exception as e:
        print(f"Storage list error for {sport}: {e}")
        return 0
    
    total_inserted = 0
    
    for file_info in data_files:
        if not check_memory():
            print(f"Skipping further files for {sport} - low memory")
            break
            
        filename = file_info.get("name","")
        storage_path = f"{folder}/{filename}"
        
        print(f"Processing (chunked): {storage_path}")
        
        try:
            # Download as bytes
            file_bytes = supabase.storage\
                .from_(BUCKET)\
                .download(storage_path)
            
            # Map row helper based on sport
            if sport == "football":
                row_mapper = map_row_football
            elif sport in ["tennis_atp", "tennis_wta", "tennis"]:
                row_mapper = map_row_tennis
            elif sport in ["nba", "basketball"]:
                row_mapper = map_row_nba
            else:
                # Simple generic row mapper
                def row_mapper(row):
                    home = row.get("home_team", row.get("home", ""))
                    away = row.get("away_team", row.get("away", ""))
                    if not home: return None
                    return {
                        "sport": sport,
                        "league": sport.upper(),
                        "home_team": str(home),
                        "away_team": str(away),
                        "match_date": str(row.get("date", "2000-01-01")),
                        "status": "completed"
                    }

            inserted = process_df_chunked(file_bytes, sport, row_mapper)
            total_inserted += inserted
            
            # Final cleanup for this file
            del file_bytes
            gc.collect()
                
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            continue
    
    print(f"Import complete: {sport} = {total_inserted} total records")
    return total_inserted

async def run_all_importers():
    """Run all sport importers sequentially"""
    sports = [
        "football",
        "tennis_atp", 
        "tennis_wta",
        "nba",
        "nfl",
        "cricket",
        "nhl",
        "mlb"
    ]
    
    total = 0
    for sport in sports:
        try:
            count = await run_single_importer(sport)
            total += count
            print(f"Completed {sport}: {count} records")
        except Exception as e:
            print(f"Failed {sport}: {e}")
            continue
    
    print(f"ALL IMPORTS COMPLETE: {total} total records")
    return total
