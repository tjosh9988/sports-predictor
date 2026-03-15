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
        sport_res = supabase.table("sports").select("id").eq("slug", sport).single().execute()
        if not sport_res.data:
            raise HTTPException(status_code=404, detail="Sport not found.")
        
        sport_id = sport_res.data["id"]
        res = (
            supabase.table("leagues")
            .select("*")
            .eq("sport_id", sport_id)
            .execute()
        )
        return res.data or []
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error fetching leagues for %s: %s", sport, exc)
        raise HTTPException(status_code=500, detail="Failed to fetch leagues.")

@router.get("/{sport}/fixtures")
async def get_sport_fixtures(sport: str, date: Optional[str] = None):
    """
    Fetch upcoming fixtures for a specific sport, optionally filtered by date.
    Returns mock data if the database is empty.
    """
    supabase = get_supabase_admin()
    try:
        sport_res = supabase.table("sports").select("id").eq("slug", sport).single().execute()
        if not sport_res.data:
            raise HTTPException(status_code=404, detail="Sport not found.")
        
        sport_id = sport_res.data["id"]
        query = supabase.table("matches").select("*").eq("sport_id", sport_id)
        
        if date:
            start_date = f"{date}T00:00:00Z"
            end_date = f"{date}T23:59:59Z"
            query = query.gte("match_date", start_date).lte("match_date", end_date)
        else:
            now = datetime.now(timezone.utc).isoformat()
            query = query.eq("status", "upcoming").gte("match_date", now)
            
        res = query.order("match_date", desc=False).execute()
        
        if res.data:
            return {"data": res.data, "count": len(res.data), "source": "database"}
            
        # Mock/Sample data if empty
        return {
            "data": [
                {
                    "id": 10001,
                    "sport_id": sport_id,
                    "league_id": 1,
                    "home_team_id": 1,
                    "away_team_id": 2,
                    "match_date": "2026-03-15T15:00:00Z",
                    "status": "upcoming",
                    "venue": "Emirates Stadium",
                    "round": "Matchday 28"
                },
                {
                    "id": 10002, 
                    "sport_id": sport_id,
                    "league_id": 2,
                    "home_team_id": 3,
                    "away_team_id": 4,
                    "match_date": "2026-03-15T20:00:00Z",
                    "status": "upcoming",
                    "venue": "Santiago Bernabeu",
                    "round": "Matchday 28"
                }
            ],
            "count": 2,
            "source": "sample"
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error fetching fixtures for %s: %s", sport, exc)
        return {"data": [], "count": 0, "error": str(exc)}
