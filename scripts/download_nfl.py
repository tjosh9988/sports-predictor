import os
import time
import requests
import pandas as pd
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
        print(f"Uploaded {filename} ({len(df)} rows)")
    except Exception as e:
        print(f"Upload error: {e}")

def download_nfl():
    print("Downloading NFL data from ESPN API...")
    all_games = []
    
    for year in range(2000, 2026):
        print(f"Fetching NFL {year}...")
        try:
            # ESPN public API - no key needed
            url = (
                f"https://site.api.espn.com/apis/site/v2"
                f"/sports/football/nfl/scoreboard"
                f"?limit=1000&dates={year}"
            )
            resp = requests.get(url, timeout=30)
            
            if resp.status_code == 200:
                data = resp.json()
                events = data.get("events", [])
                
                for event in events:
                    competitions = event.get(
                        "competitions", [{}]
                    )
                    if not competitions:
                        continue
                    
                    comp = competitions[0]
                    competitors = comp.get(
                        "competitors", []
                    )
                    
                    home = next(
                        (t for t in competitors 
                         if t.get("homeAway") == "home"),
                        {}
                    )
                    away = next(
                        (t for t in competitors 
                         if t.get("homeAway") == "away"),
                        {}
                    )
                    
                    all_games.append({
                        "date": event.get("date","")[:10],
                        "home_team": home.get(
                            "team",{}
                        ).get("displayName",""),
                        "away_team": away.get(
                            "team",{}
                        ).get("displayName",""),
                        "home_score": home.get("score"),
                        "away_score": away.get("score"),
                        "season": year,
                        "status": comp.get(
                            "status",{}
                        ).get("type",{}).get("name",""),
                    })
            
            time.sleep(1)
            
        except Exception as e:
            print(f"NFL {year} error: {e}")
            continue
    
    if all_games:
        df = pd.DataFrame(all_games)
        print(f"Total NFL games: {len(df)}")
        upload_df(df, "nfl", "all_games.csv")
    
    print("NFL download complete!")

if __name__ == "__main__":
    download_nfl()
