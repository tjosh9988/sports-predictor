"""
sports.py — FastAPI router for sports metadata and fixtures.
"""

from __future__ import annotations

import logging
from typing import List
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from app.database import get_supabase_admin
from app.schemas.schemas import SportOut, LeagueOut, MatchOut

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/sports",
    tags=["Sports"]
)

@router.get("/", response_model=List[SportOut])
async def get_sports():
    """
    List all supported sports.
    """
    supabase = get_supabase_admin()
    try:
        res = supabase.table("sports").select("*").execute()
        data = res.data or []
        return {"data": data, "count": len(data)}
    except Exception as exc:
        logger.error("Error fetching sports: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to fetch sports.")

@router.get("/{sport}/leagues", response_model=List[LeagueOut])
async def get_sport_leagues(sport: str):
    """
    Fetch all leagues for a specific sport.
    """
    supabase = get_supabase_admin()
    try:
        res = (
            supabase.table("leagues")
            .select("*")
            .eq("sport", sport)
            .execute()
        )
        return res.data or []
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error fetching leagues for %s: %s", sport, exc)
        raise HTTPException(status_code=500, detail="Failed to fetch leagues.")

@router.get("/{sport}/fixtures")
async def get_sport_fixtures(sport: str, date: str = None):
    from app.database import get_supabase_admin
    supabase = get_supabase_admin()
    
    try:
        query = supabase.table("matches")\
            .select("*")\
            .eq("status", "upcoming")\
            .order("match_date")
        
        if sport != "all":
            query = query.eq("sport", sport)
        
        if date:
            # Use date string prefix match instead of range
            query = query.gte("match_date", f"{date}T00:00:00")\
                         .lte("match_date", f"{date}T23:59:59+23:59")
        
        result = query.limit(100).execute()
        fixtures = result.data or []
        
        if fixtures:
            return {
                "data": fixtures,
                "count": len(fixtures),
                "source": "database"
            }
        
        # Debug info
        total = supabase.table("matches")\
            .select("id", count="exact")\
            .eq("status", "upcoming")\
            .execute()
        
        return {
            "data": [],
            "count": 0,
            "source": "empty",
            "debug_total_upcoming": total.count if hasattr(total, 'count') else 0,
            "date_queried": date
        }
        
    except Exception as e:
        logger.error(f"Fixtures error: {e}")
        return {"data": [], "count": 0, "error": str(e)}

def get_sample_fixtures(sport: str):
    """
    Returns a list of high-quality sample fixtures.
    """
    all_samples = [
        {
            "id": 1001,
            "sport": "football",
            "league": "Premier League",
            "home_team": "Arsenal",
            "away_team": "Chelsea",
            "match_date": "2026-03-16T15:00:00Z",
            "status": "upcoming",
            "home_odds": 2.10,
            "draw_odds": 3.40,
            "away_odds": 3.20,
            "venue": "Emirates Stadium",
            "round": "Matchday 28"
        },
        {
            "id": 1002,
            "sport": "football",
            "league": "La Liga",
            "home_team": "Real Madrid",
            "away_team": "Barcelona",
            "match_date": "2026-03-16T20:00:00Z",
            "status": "upcoming",
            "home_odds": 2.30,
            "draw_odds": 3.20,
            "away_odds": 2.90,
            "venue": "Santiago Bernabeu",
            "round": "Matchday 28"
        },
        {
            "id": 1003,
            "sport": "basketball",
            "league": "NBA",
            "home_team": "LA Lakers",
            "away_team": "Boston Celtics",
            "match_date": "2026-03-16T23:00:00Z",
            "status": "upcoming",
            "home_odds": 1.95,
            "draw_odds": None,
            "away_odds": 1.85,
            "venue": "Crypto.com Arena",
            "round": "Regular Season"
        },
        {
            "id": 1004,
            "sport": "tennis",
            "league": "ATP Miami Open",
            "home_team": "Novak Djokovic",
            "away_team": "Carlos Alcaraz",
            "match_date": "2026-03-16T18:00:00Z",
            "status": "upcoming",
            "home_odds": 2.10,
            "draw_odds": None,
            "away_odds": 1.70,
            "venue": "Hard Rock Stadium",
            "round": "Quarter Final"
        }
    ]
    
    if sport == "all":
        return all_samples
    
    # Filter by sport slug if specific sport requested
    filtered = [s for s in all_samples if s["sport"] == sport]
    return filtered if filtered else all_samples
