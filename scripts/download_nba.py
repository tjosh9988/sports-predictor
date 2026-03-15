# Requirements: pip install nba_api pandas requests supabase python-dotenv

import os
import time
import pandas as pd
from nba_api.stats.endpoints import leaguegamelog
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

if __name__ == "__main__":
    print("Downloading NBA data 2000-2025...")
    all_games = []
    
    for season_year in range(2000, 2026):
        season = f"{season_year}-{str(season_year+1)[2:]}"
        print(f"Fetching NBA {season}...")
        try:
            gamelog = leaguegamelog.LeagueGameLog(
                season=season,
                season_type_all_star="Regular Season",
                timeout=60
            )
            df = gamelog.get_data_frames()[0]
            all_games.append(df)
            print(f"Got {len(df)} games for {season}")
            time.sleep(2)
        except Exception as e:
            print(f"Error {season}: {e}")
            continue
    
    if all_games:
        combined = pd.concat(all_games, ignore_index=True)
        print(f"Total NBA games: {len(combined)}")
        upload_df(combined, "nba", "all_games.csv")
