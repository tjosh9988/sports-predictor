# Requirements: pip install "pandas<2.0" nfl-data-py requests supabase python-dotenv

import os
import pandas as pd
import nfl_data_py as nfl
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
    print("Downloading NFL data 2000-2025...")
    seasons = list(range(2000, 2026))
    
    try:
        schedules = nfl.import_schedules(seasons)
        if schedules is not None and len(schedules) > 0:
            print(f"NFL games: {len(schedules)}")
            upload_df(schedules, "nfl", "schedules.csv")
    except Exception as e:
        print(f"NFL error: {e}")
