# REQUIREMENTS:
# Python version must be 3.9, 3.10, 3.11 or 3.12
# Python 3.13+ is NOT supported by soccerdata
# 
# If you have Python 3.13+, create a virtual env:
# 
# Windows:
#   py -3.11 -m venv sports_env
#   sports_env\Scripts\activate
#   pip install -r scripts/requirements_downloader.txt
#   python scripts/download_all_sports_data.py
#
# Mac/Linux:
#   python3.11 -m venv sports_env
#   source sports_env/bin/activate
#   pip install -r scripts/requirements_downloader.txt
#   python scripts/download_all_sports_data.py

import os
import time
import requests
import pandas as pd
import io
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

supabase = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY")
)

BUCKET = "sports-data"
SEASONS = list(range(2000, 2026))

def upload_df_to_storage(
    df: pd.DataFrame, 
    sport: str, 
    filename: str
):
    """Upload DataFrame as CSV to Supabase Storage"""
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    storage_path = f"{sport}/{filename}"
    
    try:
        supabase.storage.from_(BUCKET).upload(
            path=storage_path,
            file=csv_bytes,
            file_options={
                "content-type": "text/csv",
                "upsert": "true"
            }
        )
        print(f"Uploaded: {storage_path} "
              f"({len(df)} rows)")
    except Exception as e:
        print(f"Upload error {storage_path}: {e}")

# ================================================
# FOOTBALL — using soccerdata
# ================================================
def download_football():
    print("\n=== DOWNLOADING FOOTBALL DATA ===")
    try:
        import soccerdata as sd
        
        leagues = [
            "ENG-Premier League",
            "ESP-La Liga",
            "GER-Bundesliga", 
            "ITA-Serie A",
            "FRA-Ligue 1",
            "NED-Eredivisie",
            "POR-Primeira Liga",
            "ESP-Segunda División",
            "ENG-Championship",
        ]
        
        for league in leagues:
            try:
                print(f"Fetching {league}...")
                fd = sd.FDSoccerData(
                    leagues=league,
                    seasons=list(range(2000, 2026))
                )
                matches = fd.read_schedule()
                
                if matches is not None and len(matches) > 0:
                    league_slug = league.replace(
                        " ", "_"
                    ).replace("-","_").lower()
                    upload_df_to_storage(
                        matches,
                        "football",
                        f"{league_slug}.csv"
                    )
                
                time.sleep(2)  # Rate limiting
                
            except Exception as e:
                print(f"Error {league}: {e}")
                continue
                
    except ImportError:
        print("soccerdata not installed. "
              "Run: pip install soccerdata")

# ================================================
# NBA — using nba_api (official NBA stats API)
# ================================================
def download_nba():
    print("\n=== DOWNLOADING NBA DATA ===")
    try:
        from nba_api.stats.endpoints import (
            leaguegamelog
        )
        from nba_api.stats.static import teams
        
        all_games = []
        
        for season_year in range(2000, 2026):
            season = f"{season_year}-{str(season_year+1)[2:]}"
            print(f"Fetching NBA season {season}...")
            
            try:
                gamelog = leaguegamelog.LeagueGameLog(
                    season=season,
                    season_type_all_star="Regular Season"
                )
                df = gamelog.get_data_frames()[0]
                all_games.append(df)
                time.sleep(1)
                
            except Exception as e:
                print(f"NBA season {season} error: {e}")
                continue
        
        if all_games:
            combined = pd.concat(
                all_games, ignore_index=True
            )
            print(f"Total NBA games: {len(combined)}")
            upload_df_to_storage(
                combined, "nba", "all_games.csv"
            )
            
    except ImportError:
        print("nba_api not installed. "
              "Run: pip install nba_api")

# ================================================
# NFL — using nfl_data_py 
# ================================================
def download_nfl():
    print("\n=== DOWNLOADING NFL DATA ===")
    try:
        import nfl_data_py as nfl
        
        print("Fetching NFL schedules 2000-2025...")
        seasons = list(range(2000, 2026))
        
        schedules = nfl.import_schedules(seasons)
        
        if schedules is not None and len(schedules) > 0:
            print(f"Total NFL games: {len(schedules)}")
            upload_df_to_storage(
                schedules, "nfl", "schedules.csv"
            )
        
        time.sleep(2)
        
        # Also get team stats
        print("Fetching NFL team stats...")
        team_stats = nfl.import_team_desc()
        if team_stats is not None:
            upload_df_to_storage(
                team_stats, "nfl", "team_stats.csv"
            )
            
    except ImportError:
        print("nfl_data_py not installed. "
              "Run: pip install nfl-data-py")

# ================================================
# NHL — using hockey_scraper
# ================================================
def download_nhl():
    print("\n=== DOWNLOADING NHL DATA ===")
    try:
        # Use NHL official API - completely free
        all_games = []
        
        for season_year in range(2000, 2026):
            season = f"{season_year}{season_year+1}"
            url = (
                f"https://api-web.nhle.com/v1/"
                f"standings/{season_year}-10-01"
            )
            
            # Get schedule via NHL API
            schedule_url = (
                f"https://api-web.nhle.com/v1/"
                f"schedule/{season_year}-10-01"
            )
            
            try:
                print(f"Fetching NHL {season_year}...")
                
                # Use NHL stats API
                nhl_url = (
                    f"https://statsapi.web.nhl.com"
                    f"/api/v1/schedule?"
                    f"startDate={season_year}-10-01"
                    f"&endDate={season_year+1}-06-30"
                    f"&expand=schedule.linescore"
                )
                
                resp = requests.get(
                    nhl_url, timeout=30
                )
                
                if resp.status_code == 200:
                    data = resp.json()
                    dates = data.get("dates", [])
                    
                    for date_data in dates:
                        for game in date_data.get(
                            "games", []
                        ):
                            teams_data = game.get(
                                "teams", {}
                            )
                            home = teams_data.get(
                                "home", {}
                            )
                            away = teams_data.get(
                                "away", {}
                            )
                            
                            all_games.append({
                                "date": date_data.get(
                                    "date"
                                ),
                                "home_team": home.get(
                                    "team",{}
                                ).get("name",""),
                                "away_team": away.get(
                                    "team",{}
                                ).get("name",""),
                                "home_score": home.get(
                                    "score"
                                ),
                                "away_score": away.get(
                                    "score"
                                ),
                                "season": season_year,
                            })
                
                time.sleep(1)
                
            except Exception as e:
                print(f"NHL {season_year} error: {e}")
                continue
        
        if all_games:
            df = pd.DataFrame(all_games)
            print(f"Total NHL games: {len(df)}")
            upload_df_to_storage(
                df, "nhl", "all_games.csv"
            )
            
    except Exception as e:
        print(f"NHL download error: {e}")

# ================================================
# MLB — using pybaseball
# ================================================
def download_mlb():
    print("\n=== DOWNLOADING MLB DATA ===")
    try:
        import pybaseball
        pybaseball.cache.enable()
        
        all_games = []
        
        for year in range(2000, 2026):
            try:
                print(f"Fetching MLB {year}...")
                
                # Get schedule from MLB Stats API
                url = (
                    f"https://statsapi.mlb.com/api/v1"
                    f"/schedule?sportId=1&season={year}"
                    f"&gameType=R"
                )
                resp = requests.get(url, timeout=30)
                
                if resp.status_code == 200:
                    data = resp.json()
                    dates = data.get("dates", [])
                    
                    for date_data in dates:
                        for game in date_data.get(
                            "games", []
                        ):
                            teams_data = game.get(
                                "teams", {}
                            )
                            home = teams_data.get(
                                "home", {}
                            )
                            away = teams_data.get(
                                "away", {}
                            )
                            
                            all_games.append({
                                "date": game.get(
                                    "gameDate",""
                                )[:10],
                                "home_team": home.get(
                                    "team",{}
                                ).get("name",""),
                                "away_team": away.get(
                                    "team",{}
                                ).get("name",""),
                                "home_score": home.get(
                                    "score"
                                ),
                                "away_score": away.get(
                                    "score"
                                ),
                                "season": year,
                                "venue": game.get(
                                    "venue",{}
                                ).get("name",""),
                            })
                
                time.sleep(1)
                
            except Exception as e:
                print(f"MLB {year} error: {e}")
                continue
        
        if all_games:
            df = pd.DataFrame(all_games)
            print(f"Total MLB games: {len(df)}")
            upload_df_to_storage(
                df, "mlb", "all_games.csv"
            )
            
    except ImportError:
        print("pybaseball not installed. "
              "Run: pip install pybaseball")

# ================================================
# CRICKET — from cricsheet direct download
# ================================================
def download_cricket():
    print("\n=== DOWNLOADING CRICKET DATA ===")
    try:
        import zipfile
        import json
        
        formats = {
            "tests": "https://cricsheet.org/downloads/tests_json.zip",
            "odis": "https://cricsheet.org/downloads/odis_json.zip",
            "t20s": "https://cricsheet.org/downloads/t20s_json.zip",
            "ipl": "https://cricsheet.org/downloads/ipl_json.zip",
        }
        
        for fmt, url in formats.items():
            print(f"Downloading cricket {fmt}...")
            try:
                resp = requests.get(url, timeout=120)
                if resp.status_code != 200:
                    print(f"Failed: {url}")
                    continue
                    
                zf = zipfile.ZipFile(
                    io.BytesIO(resp.content)
                )
                
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
                                    info.get("dates",
                                    ["2000-01-01"])[0]
                                ),
                                "venue": info.get(
                                    "venue", ""
                                ),
                                "competition": info.get(
                                    "competition",
                                    {}
                                ).get("type", fmt),
                                "format": fmt,
                                "winner": info.get(
                                    "outcome",{}
                                ).get("winner",""),
                            })
                    except:
                        continue
                
                if all_matches:
                    df = pd.DataFrame(all_matches)
                    print(f"Cricket {fmt}: {len(df)} matches")
                    upload_df_to_storage(
                        df, "cricket", f"{fmt}.csv"
                    )
                    
            except Exception as e:
                print(f"Cricket {fmt} error: {e}")
                continue
                
    except Exception as e:
        print(f"Cricket download error: {e}")

# ================================================
# MAIN — Run all downloads
# ================================================
if __name__ == "__main__":
    print("SPORTS DATA DOWNLOADER")
    print("Downloading 2000-2025 for all sports")
    print("="*50)
    
    # Run each sport
    download_football()
    download_nba()
    download_nfl()
    download_nhl()
    download_mlb()
    download_cricket()
    
    print("\n" + "="*50)
    print("ALL DOWNLOADS COMPLETE")
    print("Check Supabase Storage for uploaded files")
