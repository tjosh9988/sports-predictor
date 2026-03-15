# Requirements: pip install pandas requests supabase python-dotenv

import os
import time
import json
import zipfile
import io
import pandas as pd
import requests
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

def download_nhl():
    print("Downloading NHL data...")
    all_games = []
    
    for season_year in range(2000, 2025):
        try:
            url = (
                f"https://statsapi.web.nhl.com/api/v1/schedule"
                f"?startDate={season_year}-10-01"
                f"&endDate={season_year+1}-06-30"
            )
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                for date_data in data.get("dates", []):
                    for game in date_data.get("games", []):
                        teams = game.get("teams", {})
                        home = teams.get("home", {})
                        away = teams.get("away", {})
                        all_games.append({
                            "date": date_data.get("date"),
                            "home_team": home.get("team",{}).get("name",""),
                            "away_team": away.get("team",{}).get("name",""),
                            "home_score": home.get("score"),
                            "away_score": away.get("score"),
                            "season": season_year,
                        })
            time.sleep(1)
            print(f"NHL {season_year}: done")
        except Exception as e:
            print(f"NHL {season_year} error: {e}")
    
    if all_games:
        df = pd.DataFrame(all_games)
        print(f"Total NHL games: {len(df)}")
        upload_df(df, "nhl", "all_games.csv")

def download_mlb():
    print("Downloading MLB data...")
    all_games = []
    
    for year in range(2000, 2026):
        try:
            url = (
                f"https://statsapi.mlb.com/api/v1/schedule"
                f"?sportId=1&season={year}&gameType=R"
            )
            resp = requests.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                for date_data in data.get("dates", []):
                    for game in date_data.get("games", []):
                        teams = game.get("teams", {})
                        home = teams.get("home", {})
                        away = teams.get("away", {})
                        all_games.append({
                            "date": game.get("gameDate","")[:10],
                            "home_team": home.get("team",{}).get("name",""),
                            "away_team": away.get("team",{}).get("name",""),
                            "home_score": home.get("score"),
                            "away_score": away.get("score"),
                            "season": year,
                            "venue": game.get("venue",{}).get("name",""),
                        })
            time.sleep(0.5)
            print(f"MLB {year}: done")
        except Exception as e:
            print(f"MLB {year} error: {e}")
    
    if all_games:
        df = pd.DataFrame(all_games)
        print(f"Total MLB games: {len(df)}")
        upload_df(df, "mlb", "all_games.csv")

def download_cricket():
    print("Downloading cricket data...")
    formats = {
        "tests": "https://cricsheet.org/downloads/tests_json.zip",
        "odis": "https://cricsheet.org/downloads/odis_json.zip",
        "t20s": "https://cricsheet.org/downloads/t20s_json.zip",
        "ipl": "https://cricsheet.org/downloads/ipl_json.zip",
    }
    
    for fmt, url in formats.items():
        try:
            print(f"Downloading cricket {fmt}...")
            resp = requests.get(url, timeout=120)
            if resp.status_code != 200:
                continue
            
            zf = zipfile.ZipFile(io.BytesIO(resp.content))
            all_matches = []
            
            for name in zf.namelist():
                if not name.endswith('.json'):
                    continue
                try:
                    with zf.open(name) as f:
                        data = json.load(f)
                    info = data.get("info", {})
                    teams = info.get("teams", [])
                    if len(teams) >= 2:
                        all_matches.append({
                            "home_team": teams[0],
                            "away_team": teams[1],
                            "match_date": str(
                                info.get("dates",["2000-01-01"])[0]
                            ),
                            "venue": info.get("venue",""),
                            "format": fmt,
                            "winner": info.get("outcome",{}).get("winner",""),
                        })
                except:
                    continue
            
            if all_matches:
                df = pd.DataFrame(all_matches)
                print(f"Cricket {fmt}: {len(df)} matches")
                upload_df(df, "cricket", f"{fmt}.csv")
                
        except Exception as e:
            print(f"Cricket {fmt} error: {e}")

if __name__ == "__main__":
    download_nhl()
    download_mlb()
    download_cricket()
