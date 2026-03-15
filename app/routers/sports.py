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
        return res.data or []
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

@router.get("/{sport}/fixtures", response_model=List[MatchOut])
async def get_sport_fixtures(sport: str):
    """
    Fetch upcoming fixtures for a specific sport.
    (Aliased to predictions/fixtures for convenience)
    """
    supabase = get_supabase_admin()
    try:
        sport_res = supabase.table("sports").select("id").eq("slug", sport).single().execute()
        if not sport_res.data:
            raise HTTPException(status_code=404, detail="Sport not found.")
        
        sport_id = sport_res.data["id"]
        now = datetime.now(timezone.utc).isoformat()
        
        res = (
            supabase.table("matches")
            .select("*")
            .eq("sport_id", sport_id)
            .eq("status", "upcoming")
            .gte("match_date", now)
            .order("match_date", desc=False)
            .execute()
        )
        return res.data or []
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error fetching fixtures for %s: %s", sport, exc)
        raise HTTPException(status_code=500, detail="Failed to fetch fixtures.")
