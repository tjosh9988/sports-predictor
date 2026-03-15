# Requirements: pip install soccerdata pandas requests supabase python-dotenv

import os
import time
import pandas as pd
import requests
import io
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
)

BUCKET = "sports-data"

def upload_df(df, sport, filename):
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    try:
        supabase.storage.from_(BUCKET).upload(
            path=f"{sport}/{filename}",
            file=csv_bytes,
            file_options={
                "content-type": "text/csv",
                "upsert": "true"
            }
        )
        print(f"Uploaded {sport}/{filename} ({len(df)} rows)")
    except Exception as e:
        print(f"Upload error: {e}")

def download_football_direct():
    """Download directly from football-data.co.uk"""
    print("Downloading football data...")
    
    # All available leagues on football-data.co.uk
    leagues = {
        "E0": "premier_league",
        "E1": "championship",
        "E2": "league_one",
        "E3": "league_two",
        "SC0": "scottish_premier",
        "D1": "bundesliga",
        "D2": "bundesliga2",
        "SP1": "la_liga",
        "SP2": "la_liga2",
        "I1": "serie_a",
        "I2": "serie_b",
        "F1": "ligue_1",
        "F2": "ligue_2",
        "N1": "eredivisie",
        "P1": "primeira_liga",
        "T1": "super_lig",
        "G1": "super_league_greece",
    }
    
    # Seasons from 2000 to 2025
    seasons = []
    for year in range(0, 26):
        start = str(year).zfill(2)
        end = str(year + 1).zfill(2)
        seasons.append(f"{start}{end}")
    
    all_matches = []
    
    for league_code, league_name in leagues.items():
        league_matches = []
        
        for season in seasons:
            url = (
                f"https://www.football-data.co.uk"
                f"/mmz4281/{season}/{league_code}.csv"
            )
            
            try:
                resp = requests.get(url, timeout=15)
                if resp.status_code == 200:
                    df = pd.read_csv(
                        io.StringIO(resp.text),
                        low_memory=False
                    )
                    if len(df) > 0 and "HomeTeam" in df.columns:
                        df["league_code"] = league_code
                        df["league_name"] = league_name
                        df["season"] = f"20{season[:2]}/20{season[2:]}"
                        league_matches.append(df)
                        print(f"Got {league_code} {season}: {len(df)} matches")
                
                time.sleep(0.5)
                
            except Exception as e:
                continue
        
        if league_matches:
            league_df = pd.concat(
                league_matches, ignore_index=True
            )
            upload_df(league_df, "football", f"{league_name}.csv")
            all_matches.extend(league_matches)
    
    print(f"Football download complete!")

if __name__ == "__main__":
    download_football_direct()
