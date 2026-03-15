import os
import io
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

def process_football_df(df: pd.DataFrame) -> list:
    matches = []
    for _, row in df.iterrows():
        try:
            home = str(row.get("HomeTeam",
                       row.get("home_team",
                       row.get("Home",""))))
            away = str(row.get("AwayTeam", 
                       row.get("away_team",
                       row.get("Away",""))))
            
            if not home or not away or home == "nan":
                continue
            
            match = {
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
                "home_score": _safe_int(row.get("FTHG",
                                        row.get("HG"))),
                "away_score": _safe_int(row.get("FTAG",
                                        row.get("AG"))),
                "result": str(row.get("FTR",
                              row.get("Res",""))),
                "home_shots": _safe_int(row.get("HS")),
                "away_shots": _safe_int(row.get("AS")),
                "home_corners": _safe_int(row.get("HC")),
                "away_corners": _safe_int(row.get("AC")),
                "home_yellow_cards": _safe_int(
                    row.get("HY")
                ),
                "away_yellow_cards": _safe_int(
                    row.get("AY")
                ),
                "home_odds": _safe_float(
                    row.get("B365H",row.get("BbAvH"))
                ),
                "draw_odds": _safe_float(
                    row.get("B365D",row.get("BbAvD"))
                ),
                "away_odds": _safe_float(
                    row.get("B365A",row.get("BbAvA"))
                ),
            }
            matches.append(match)
        except:
            continue
    return matches

def process_tennis_df(
    df: pd.DataFrame, 
    tour: str
) -> list:
    matches = []
    for _, row in df.iterrows():
        try:
            winner = str(row.get("winner_name",
                         row.get("player1","")))
            loser = str(row.get("loser_name",
                        row.get("player2","")))
            
            if not winner or winner == "nan":
                continue
                
            match = {
                "sport": "tennis",
                "league": str(row.get("tourney_name",
                              row.get("tournament",
                              "Unknown"))),
                "home_team": winner,
                "away_team": loser,
                "match_date": str(row.get("tourney_date",
                                  row.get("date",
                                  "2000-01-01"))),
                "status": "completed",
                "result": "H",
                "venue": str(row.get("surface","")),
            }
            matches.append(match)
        except:
            continue
    return matches

def process_nba_df(df: pd.DataFrame) -> list:
    matches = []
    for _, row in df.iterrows():
        try:
            home = str(row.get("home_team",
                       row.get("TEAM_NAME",
                       row.get("team_name_home",""))))
            away = str(row.get("visitor_team",
                       row.get("team_name_away",
                       row.get("MATCHUP",""))))
            
            if not home or home == "nan":
                continue
                
            match = {
                "sport": "basketball",
                "league": "NBA",
                "home_team": home,
                "away_team": away,
                "match_date": str(row.get("game_date",
                                  row.get("GAME_DATE",
                                  "2000-01-01"))),
                "status": "completed",
                "home_score": _safe_int(
                    row.get("home_team_score",
                    row.get("PTS"))
                ),
                "away_score": _safe_int(
                    row.get("visitor_team_score")
                ),
            }
            if home != "Unknown":
                matches.append(match)
        except:
            continue
    return matches

def process_generic_df(
    df: pd.DataFrame, 
    sport: str
) -> list:
    matches = []
    
    # Common column patterns
    home_cols = ["home_team","HomeTeam","home",
                 "team1","Team1","batting_team"]
    away_cols = ["away_team","AwayTeam","away",
                 "team2","Team2","bowling_team"]
    date_cols = ["date","Date","game_date",
                 "match_date","tourney_date"]
    
    home_col = next(
        (c for c in home_cols if c in df.columns),None
    )
    away_col = next(
        (c for c in away_cols if c in df.columns),None
    )
    date_col = next(
        (c for c in date_cols if c in df.columns),None
    )
    
    if not home_col or not away_col:
        print(f"Cannot find team columns for {sport}")
        print(f"Available: {list(df.columns)}")
        return []
    
    for _, row in df.iterrows():
        try:
            home = str(row[home_col])
            away = str(row[away_col])
            if home == "nan" or not home:
                continue
            match = {
                "sport": sport,
                "league": sport.upper(),
                "home_team": home,
                "away_team": away,
                "match_date": str(row[date_col]) 
                              if date_col 
                              else "2000-01-01",
                "status": "completed",
            }
            matches.append(match)
        except:
            continue
    return matches

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
        filename = file_info.get("name","")
        storage_path = f"{folder}/{filename}"
        
        print(f"Processing: {storage_path}")
        
        try:
            # Download as bytes
            file_bytes = supabase.storage\
                .from_(BUCKET)\
                .download(storage_path)
            
            # Read CSV from bytes directly
            df = pd.read_csv(
                io.BytesIO(file_bytes),
                low_memory=False
            )
            
            print(f"Read {len(df)} rows from {filename}")
            
            # Process based on sport
            matches = []
            if sport == "football":
                matches = process_football_df(df)
            elif sport in ["tennis_atp", "tennis_wta", "tennis"]:
                matches = process_tennis_df(df, sport)
            elif sport in ["nba", "basketball"]:
                matches = process_nba_df(df)
            else:
                matches = process_generic_df(df, sport)
            
            print(f"Processed {len(matches)} matches "
                  f"from {filename}")
            
            if matches:
                inserted = insert_matches_batch(
                    matches, sport
                )
                total_inserted += inserted
                
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print(f"Import complete: {sport} = "
          f"{total_inserted} total records")
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
