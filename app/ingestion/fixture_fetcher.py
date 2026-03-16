import os
import httpx
import asyncio
from datetime import datetime, timedelta
from app.database import get_supabase_admin

API_KEY = os.getenv("API_SPORTS_KEY", "")

HEADERS = {
    "x-apisports-key": API_KEY
}

# API-Sports endpoints per sport
SPORT_ENDPOINTS = {
    "football": {
        "url": "https://v3.football.api-sports.io/fixtures",
        "params": {"next": 50},
        "mapper": "map_football_fixture"
    },
    "basketball": {
        "url": "https://v1.basketball.api-sports.io/games",
        "params": {"next": 50, "league": "12", "season": "2024-2025"},
        "mapper": "map_basketball_fixture"
    },
    "nfl": {
        "url": "https://v1.american-football.api-sports.io/games",
        "params": {"league": "1", "season": "2025"},
        "mapper": "map_generic_fixture"
    },
    "hockey": {
        "url": "https://v1.hockey.api-sports.io/games",
        "params": {"next": 50},
        "mapper": "map_generic_fixture"
    },
    "baseball": {
        "url": "https://v2.baseball.api-sports.io/games",
        "params": {"next": 50},
        "mapper": "map_generic_fixture"
    },
}

def map_football_fixture(f: dict) -> dict:
    fixture = f.get("fixture", {})
    teams = f.get("teams", {})
    goals = f.get("goals", {})
    league = f.get("league", {})
    
    match_date = fixture.get("date", "")
    status_long = fixture.get("status", {}).get("long", "")
    
    # Determine status
    if status_long in ["Not Started", "Time to be Defined"]:
        status = "upcoming"
    elif status_long in ["Match Finished", "Full Time"]:
        status = "completed"
    else:
        status = "live"
    
    return {
        "sport": "football",
        "league": league.get("name", "Unknown"),
        "home_team": teams.get("home", {}).get("name", ""),
        "away_team": teams.get("away", {}).get("name", ""),
        "match_date": match_date,
        "status": status,
        "home_score": goals.get("home"),
        "away_score": goals.get("away"),
        "venue": fixture.get("venue", {}).get("name", ""),
        "round": league.get("round", ""),
    }

def map_basketball_fixture(f: dict) -> dict:
    teams = f.get("teams", {})
    scores = f.get("scores", {})
    league = f.get("league", {})
    
    return {
        "sport": "basketball",
        "league": league.get("name", "NBA"),
        "home_team": teams.get("home", {}).get("name", ""),
        "away_team": teams.get("away", {}).get("name", ""),
        "match_date": f.get("date", ""),
        "status": "upcoming",
        "home_score": scores.get("home", {}).get("total"),
        "away_score": scores.get("away", {}).get("total"),
    }

def map_generic_fixture(f: dict) -> dict:
    teams = f.get("teams", {})
    scores = f.get("scores", {})
    
    return {
        "sport": "nfl",
        "league": "NFL",
        "home_team": teams.get("home", {}).get("name", ""),
        "away_team": teams.get("away", {}).get("name", ""),
        "match_date": f.get("date", ""),
        "status": "upcoming",
        "home_score": scores.get("home", {}).get("total"),
        "away_score": scores.get("away", {}).get("total"),
    }

async def fetch_sport_fixtures(
    sport: str, 
    url: str, 
    params: dict,
    mapper_name: str
) -> int:
    """Fetch and store fixtures for one sport"""
    # Resolve mapper function from string name
    mappers = {
        "map_football_fixture": map_football_fixture,
        "map_basketball_fixture": map_basketball_fixture,
        "map_generic_fixture": map_generic_fixture
    }
    mapper = mappers.get(mapper_name)
    
    supabase = get_supabase_admin()
    inserted = 0
    
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                url, 
                headers=HEADERS,
                params=params
            )
            
            if resp.status_code != 200:
                print(f"{sport} API error: {resp.status_code}")
                print(f"Response: {resp.text[:200]}")
                return 0
            
            data = resp.json()
            
            # Check API errors
            errors = data.get("errors", {})
            if errors:
                print(f"{sport} API errors: {errors}")
                return 0
            
            fixtures = data.get("response", [])
            print(f"{sport}: Got {len(fixtures)} fixtures from API")
            
            for f in fixtures:
                try:
                    match = mapper(f)
                    
                    if not match["home_team"] or not match["away_team"]:
                        continue
                    
                    # Get odds if available
                    odds_data = f.get("odds", [])
                    if odds_data:
                        for odd in odds_data:
                            for value in odd.get("values", []):
                                if value.get("value") == "Home":
                                    match["home_odds"] = float(value.get("odd", 2.0))
                                elif value.get("value") == "Away":
                                    match["away_odds"] = float(value.get("odd", 2.0))
                                elif value.get("value") == "Draw":
                                    match["draw_odds"] = float(value.get("odd", 3.0))
                    
                    supabase.table("matches").upsert(
                        match,
                        on_conflict="sport,home_team,away_team,match_date"
                    ).execute()
                    inserted += 1
                    
                except Exception as e:
                    print(f"Fixture insert error: {e}")
                    continue
            
    except Exception as e:
        print(f"{sport} fetch error: {e}")
    
    return inserted

async def fetch_football_with_odds():
    """Fetch football fixtures with separate odds call"""
    supabase = get_supabase_admin()
    inserted = 0
    
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # Get next 50 football fixtures
            resp = await client.get(
                "https://v3.football.api-sports.io/fixtures",
                headers=HEADERS,
                params={"next": 50}
            )
            
            if resp.status_code != 200:
                print(f"Football API status: {resp.status_code}")
                return 0
            
            data = resp.json()
            fixtures = data.get("response", [])
            print(f"Football: {len(fixtures)} upcoming fixtures")
            
            for f in fixtures:
                match = map_football_fixture(f)
                
                if not match["home_team"]:
                    continue
                
                # Get fixture ID for odds lookup
                fixture_id = f.get("fixture", {}).get("id")
                
                # Try to get odds
                if fixture_id:
                    try:
                        odds_resp = await client.get(
                            "https://v3.football.api-sports.io/odds",
                            headers=HEADERS,
                            params={
                                "fixture": fixture_id,
                                "bookmaker": 6  # Bet365
                            }
                        )
                        if odds_resp.status_code == 200:
                            odds_data = odds_resp.json()
                            odds_response = odds_data.get("response", [])
                            if odds_response:
                                bks = odds_response[0].get("bookmakers", [])
                                if bks:
                                    bets = bks[0].get("bets", [])
                                    for bet in bets:
                                        if bet.get("name") == "Match Winner":
                                            for v in bet.get("values", []):
                                                if v.get("value") == "Home":
                                                    match["home_odds"] = float(v.get("odd", 2.0))
                                                elif v.get("value") == "Away":
                                                    match["away_odds"] = float(v.get("odd", 2.0))
                                                elif v.get("value") == "Draw":
                                                    match["draw_odds"] = float(v.get("odd", 3.0))
                        await asyncio.sleep(0.5)  # Rate limit
                    except:
                        pass
                
                try:
                    supabase.table("matches").insert(match).execute()
                    inserted += 1
                except Exception as e:
                    # Try upsert if insert fails
                    try:
                        supabase.table("matches").upsert(match).execute()
                        inserted += 1
                    except:
                        pass
            
    except Exception as e:
        print(f"Football fetch error: {e}")
    
    return inserted

async def fetch_all_fixtures():
    """Fetch real upcoming fixtures from all sports"""
    print("Fetching REAL fixtures from API-Sports...")
    total = 0
    
    # Football with odds
    count = await fetch_football_with_odds()
    total += count
    print(f"Football fixtures: {count}")
    
    # Other sports
    other_sports = [
        ("basketball", 
         "https://v1.basketball.api-sports.io/games",
         {"next": 50, "league": "12", "season": "2024-2025"},
         "map_basketball_fixture"),
    ]
    
    for sport, url, params, mapper_name in other_sports:
        try:
            count = await fetch_sport_fixtures(
                sport, url, params, mapper_name
            )
            total += count
            print(f"{sport} fixtures: {count}")
            await asyncio.sleep(1)
        except Exception as e:
            print(f"{sport} error: {e}")
    
    print(f"Total real fixtures fetched: {total}")
    
    # Verify what was saved
    supabase = get_supabase_admin()
    result = supabase.table("matches")\
        .select("id", count="exact")\
        .eq("status", "upcoming")\
        .execute()
    print(f"Total upcoming in DB: {result.count}")
    
    return total

async def create_upcoming_fixtures_from_history():
    """Only used as last resort fallback"""
    print("WARNING: Using history-based fixture fallback")
    return 0
