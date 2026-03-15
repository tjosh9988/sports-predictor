import os
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

def process_football_file(local_path: str) -> list:
    """Process a football CSV file"""
    matches = []
    try:
        df = pd.read_csv(local_path, low_memory=False)
        print(f"Processing {len(df)} football rows from "
              f"{local_path}")
        
        for _, row in df.iterrows():
            try:
                # football-data.co.uk format
                match = {
                    "sport": "football",
                    "league": str(row.get("Div", 
                                  row.get("League", 
                                  "Unknown"))),
                    "home_team": str(row.get("HomeTeam",
                                    row.get("home_team",
                                    "Unknown"))),
                    "away_team": str(row.get("AwayTeam",
                                    row.get("away_team",
                                    "Unknown"))),
                    "match_date": str(row.get("Date",
                                     row.get("date",
                                     datetime.now()
                                     .isoformat()))),
                    "status": "completed",
                    "home_score": int(row["FTHG"]) 
                                  if "FTHG" in row 
                                  and pd.notna(row["FTHG"]) 
                                  else None,
                    "away_score": int(row["FTAG"]) 
                                  if "FTAG" in row 
                                  and pd.notna(row["FTAG"]) 
                                  else None,
                    "result": str(row.get("FTR", "")),
                    "home_shots": int(row["HS"]) 
                                  if "HS" in row 
                                  and pd.notna(row["HS"]) 
                                  else None,
                    "away_shots": int(row["AS"]) 
                                  if "AS" in row 
                                  and pd.notna(row["AS"]) 
                                  else None,
                    "home_corners": int(row["HC"]) 
                                    if "HC" in row 
                                    and pd.notna(row["HC"]) 
                                    else None,
                    "away_corners": int(row["AC"]) 
                                    if "AC" in row 
                                    and pd.notna(row["AC"]) 
                                    else None,
                    "home_yellow_cards": int(row["HY"]) 
                                         if "HY" in row 
                                         and pd.notna(row["HY"]) 
                                         else None,
                    "away_yellow_cards": int(row["AY"]) 
                                         if "AY" in row 
                                         and pd.notna(row["AY"]) 
                                         else None,
                    "home_odds": float(row["B365H"]) 
                                 if "B365H" in row 
                                 and pd.notna(row["B365H"]) 
                                 else None,
                    "draw_odds": float(row["B365D"]) 
                                 if "B365D" in row 
                                 and pd.notna(row["B365D"]) 
                                 else None,
                    "away_odds": float(row["B365A"]) 
                                 if "B365A" in row 
                                 and pd.notna(row["B365A"]) 
                                 else None,
                }
                
                # Skip rows with no team names
                if (match["home_team"] == "Unknown" or 
                    match["away_team"] == "Unknown"):
                    continue
                    
                matches.append(match)
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"Error processing football file: {e}")
    
    return matches

def process_tennis_file(
    local_path: str, 
    tour: str = "atp"
) -> list:
    """Process a tennis CSV file"""
    matches = []
    try:
        df = pd.read_csv(local_path, low_memory=False)
        
        for _, row in df.iterrows():
            try:
                match = {
                    "sport": "tennis",
                    "league": str(row.get("tourney_name",
                                  "Unknown")),
                    "home_team": str(row.get("winner_name",
                                    "Unknown")),
                    "away_team": str(row.get("loser_name",
                                    "Unknown")),
                    "match_date": str(row.get("tourney_date",
                                     datetime.now()
                                     .isoformat())),
                    "status": "completed",
                    "result": "H",
                    "venue": str(row.get("surface", "")),
                }
                if (match["home_team"] != "Unknown" and 
                    match["away_team"] != "Unknown"):
                    matches.append(match)
            except:
                continue
    except Exception as e:
        print(f"Tennis file error: {e}")
    
    return matches

def process_nba_file(local_path: str) -> list:
    """Process an NBA CSV file"""
    matches = []
    try:
        df = pd.read_csv(local_path, low_memory=False)
        
        for _, row in df.iterrows():
            try:
                match = {
                    "sport": "basketball",
                    "league": "NBA",
                    "home_team": str(row.get("home_team",
                                    row.get("TEAM_NAME",
                                    "Unknown"))),
                    "away_team": str(row.get("visitor_team",
                                    row.get("MATCHUP",
                                    "Unknown"))),
                    "match_date": str(row.get("game_date",
                                     row.get("GAME_DATE",
                                     datetime.now()
                                     .isoformat()))),
                    "status": "completed",
                    "home_score": int(row["home_team_score"]) 
                                  if "home_team_score" in row 
                                  and pd.notna(
                                    row["home_team_score"]
                                  ) else None,
                    "away_score": int(row["visitor_team_score"]) 
                                  if "visitor_team_score" in row 
                                  and pd.notna(
                                    row["visitor_team_score"]
                                  ) else None,
                }
                if match["home_team"] != "Unknown":
                    matches.append(match)
            except:
                continue
    except Exception as e:
        print(f"NBA file error: {e}")
    
    return matches

def process_generic_file(
    local_path: str, 
    sport: str
) -> list:
    """Generic processor for NFL, Cricket, NHL, MLB"""
    matches = []
    try:
        df = pd.read_csv(local_path, low_memory=False)
        print(f"Processing {len(df)} rows for {sport}")
        
        # Try common column name patterns
        home_cols = ["home_team", "HomeTeam", "home", 
                     "team1", "batting_team"]
        away_cols = ["away_team", "AwayTeam", "away", 
                     "team2", "bowling_team"]
        date_cols = ["date", "Date", "game_date", 
                     "match_date", "tourney_date"]
        
        home_col = next(
            (c for c in home_cols if c in df.columns), 
            None
        )
        away_col = next(
            (c for c in away_cols if c in df.columns), 
            None
        )
        date_col = next(
            (c for c in date_cols if c in df.columns), 
            None
        )
        
        if not home_col or not away_col:
            print(f"Cannot find team columns for {sport}")
            print(f"Available columns: {list(df.columns)}")
            return []
        
        for _, row in df.iterrows():
            try:
                match = {
                    "sport": sport,
                    "league": sport.upper(),
                    "home_team": str(row[home_col]),
                    "away_team": str(row[away_col]),
                    "match_date": str(row[date_col]) 
                                  if date_col 
                                  else datetime.now()
                                  .isoformat(),
                    "status": "completed",
                }
                matches.append(match)
            except:
                continue
                
    except Exception as e:
        print(f"Generic processor error for {sport}: {e}")
    
    return matches

async def run_single_importer(sport: str):
    folder = FOLDER_MAP.get(sport, sport)
    print(f"Starting import for: {sport} -> folder: {folder}")
    
    supabase = get_supabase_admin()
    try:
        files = supabase.storage\
            .from_(BUCKET)\
            .list(folder)
        
        if not files:
            print(f"No files found in storage/{folder}")
            return 0
            
        # Filter only CSV/JSON
        data_files = [
            f for f in files 
            if f.get("name","").endswith(('.csv','.json'))
        ]
        
        print(f"Found {len(data_files)} files in {folder}")
        
    except Exception as e:
        print(f"Storage list error: {e}")
        return 0
    
    total_inserted = 0
    for file_info in data_files:
        filename = file_info.get("name","")
        storage_path = f"{folder}/{filename}"
        local_path = f"/tmp/{sport}_{filename}"
        
        try:
            # Download from storage
            print(f"Downloading: {storage_path}")
            data = supabase.storage\
                .from_(BUCKET)\
                .download(storage_path)
            
            with open(local_path, "wb") as f:
                f.write(data)
            
            # Process based on sport
            if sport in ["football"]:
                matches = process_football_file(local_path)
            elif sport in ["tennis", "tennis_atp", 
                           "tennis_wta"]:
                matches = process_tennis_file(local_path)
            elif sport in ["nba", "basketball"]:
                matches = process_nba_file(local_path)
            else:
                matches = process_generic_file(
                    local_path, sport
                )
            
            if matches:
                inserted = insert_matches_batch(
                    matches, sport
                )
                total_inserted += inserted
                
        except Exception as e:
            print(f"Error processing {filename}: {e}")
        finally:
            try:
                os.remove(local_path)
            except:
                pass
    
    print(f"Import complete: {sport} = {total_inserted} records")
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
