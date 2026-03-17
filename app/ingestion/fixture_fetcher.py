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
    from datetime import datetime, timedelta
    import httpx
    from app.database import get_supabase_admin
    
    supabase = get_supabase_admin()
    inserted = 0
    
    # Fetch fixtures for next 7 days
    # using date parameter (works on free plan)
    today = datetime.now()
    
    async with httpx.AsyncClient(timeout=30) as client:
        for days_ahead in range(0, 2):
            target_date = today + timedelta(days=days_ahead)
            date_str = target_date.strftime("%Y-%m-%d")
            
            print(f"Fetching football fixtures for {date_str}...")
            
            try:
                resp = await client.get(
                    "https://v3.football.api-sports.io/fixtures",
                    headers={"x-apisports-key": API_KEY},
                    params={"date": date_str}
                )
                
                if resp.status_code != 200:
                    print(f"Error {resp.status_code} for {date_str}")
                    continue
                
                data = resp.json()
                errors = data.get("errors", {})
                
                if errors:
                    print(f"API errors for {date_str}: {errors}")
                    continue
                
                fixtures = data.get("response", [])
                print(f"{date_str}: {len(fixtures)} fixtures")
                
                for f in fixtures:
                    try:
                        match = map_football_fixture(f)
                        if not match["home_team"]:
                            continue
                        
                        supabase.table("matches")\
                            .upsert(match)\
                            .execute()
                        inserted += 1
                    except Exception as e:
                        continue
                
                # Rate limit — 100 req/day = careful
                await asyncio.sleep(2)
                
            except Exception as e:
                print(f"Date {date_str} error: {e}")
                continue
    
    print(f"Football: {inserted} fixtures inserted")
    return inserted

async def fetch_all_fixtures():
    print("Fetching real fixtures from API-Sports...")
    total = 0
    
    # Football - primary sport
    count = await fetch_football_with_odds()
    total += count
    print(f"Football fixtures: {count}")
    
    # Basketball (NBA)
    try:
        count = await fetch_sport_fixtures(
            "basketball",
            "https://v1.basketball.api-sports.io/games",
            {"league": "12", "season": "2024-2025",
             "date": datetime.now().strftime("%Y-%m-%d")},
            "map_basketball_fixture"
        )
        total += count
        print(f"Basketball: {count}")
    except Exception as e:
        print(f"Basketball error: {e}")
        
    print(f"Total fixtures fetched: {total}")
    return total

async def create_upcoming_fixtures_from_history():
    """Only used as last resort fallback"""
    print("WARNING: Using history-based fixture fallback")
    return 0
