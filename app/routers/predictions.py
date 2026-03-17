"""
predictions.py — FastAPI router for match predictions and accumulators.
"""

from __future__ import annotations

import logging
from typing import List, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from app.database import get_supabase_admin
from app.schemas.schemas import PredictionOut, AccumulatorOut, MatchOut
from app.redis_client import get_redis

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/predictions",
    tags=["Predictions"]
)

@router.get("/accumulators/today")
async def get_today_accumulators(background_tasks: BackgroundTasks):
    """
    Fetch all accumulators generated in the last 24 hours.
    Triggers generation if empty.
    """
    supabase = get_supabase_admin()
    redis = get_redis()
    
    now = datetime.now(timezone.utc)
    day_ago = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    
    try:
        res = (
            supabase.table("accumulators")
            .select("*, legs:accumulator_legs(*)")
            .gte("created_at", day_ago)
            .order("created_at", desc=True)
            .execute()
        )
        
        if res.data:
            # Filter to one per type
            seen_types = set()
            filtered = []
            for acca in res.data:
                acca_type = acca.get("acca_type")
                if acca_type not in seen_types:
                    filtered.append(acca)
                    seen_types.add(acca_type)
            
            return {"data": filtered, "count": len(filtered), "source": "database"}
            
        # check if already generating
        is_generating = redis.get("acca_generation_in_progress")
        if is_generating:
            return {
                "status": "generating", 
                "message": "We are fetching real data. It will be loaded once it is completed.",
                "data": [],
                "count": 0
            }
        
        # Trigger generation
        from app.services.accumulator_builder import build_all_accumulators
        background_tasks.add_task(build_all_accumulators)
        
        # Set flag in redis for 15 mins
        redis.setex("acca_generation_in_progress", 900, "true")
        
        return {
            "status": "generating",
            "message": "We are fetching real data. It will be loaded once it is completed.",
            "data": [],
            "count": 0
        }
    except Exception as exc:
        logger.error("Error fetching today's accumulators: %s", exc)
        return {"data": [], "count": 0, "error": str(exc)}

@router.get("/accumulators/{acca_type}")
async def get_accumulators_by_type(acca_type: str, background_tasks: BackgroundTasks):
    """
    Fetch all accumulators of a specific type (3odds, 5odds, 10odds).
    """
    if acca_type not in ["3odds", "5odds", "10odds"]:
        raise HTTPException(status_code=400, detail="Invalid accumulator type.")
        
    supabase = get_supabase_admin()
    redis = get_redis()
    
    try:
        res = (
            supabase.table("accumulators")
            .select("*, legs:accumulator_legs(*)")
            .eq("acca_type", acca_type)
            .order("created_at", desc=True)
            .limit(10)
            .execute()
        )
        data = res.data or []
        
        if not data:
            # check if today's accas exist at all
            now = datetime.now(timezone.utc)
            day_ago = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            
            check_any = supabase.table("accumulators").select("id").gte("created_at", day_ago).limit(1).execute()
            
            if not check_any.data:
                is_generating = redis.get("acca_generation_in_progress")
                if is_generating:
                    return {
                        "status": "generating",
                        "message": "We are fetching real data. It will be loaded once it is completed.",
                        "data": [],
                        "count": 0
                    }
                
                # Trigger generation
                from app.services.accumulator_builder import build_all_accumulators
                background_tasks.add_task(build_all_accumulators)
                redis.setex("acca_generation_in_progress", 900, "true")
                
                return {
                    "status": "generating",
                    "message": "We are fetching real data. It will be loaded once it is completed.",
                    "data": [],
                    "count": 0
                }
                
        return {"data": data, "count": len(data)}
    except Exception as exc:
        logger.error("Error fetching %s accumulators: %s", acca_type, exc)
        raise HTTPException(status_code=500, detail=f"Failed to fetch {acca_type} accumulators.")

@router.get("/fixtures/{sport}")
async def get_sport_fixtures(sport: str):
    """
    Fetch upcoming fixtures for a specific sport.
    """
    supabase = get_supabase_admin()
    try:
        now = datetime.now(timezone.utc).isoformat()
        
        res = (
            supabase.table("matches")
            .select("*")
            .eq("sport", sport)
            .eq("status", "upcoming")
            .gte("match_date", now)
            .order("match_date", desc=False)
            .limit(50)
            .execute()
        )
        return res.data or []
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error fetching fixtures for %s: %s", sport, exc)
        raise HTTPException(status_code=500, detail="Failed to fetch fixtures.")

@router.get("/match/{match_id}")
async def get_match_prediction(match_id: int):
    """
    Fetch a specific match and all its associated predictions.
    """
    supabase = get_supabase_admin()
    try:
        # Fetch match
        match_res = supabase.table("matches").select("*").eq("id", match_id).single().execute()
        if not match_res.data:
            raise HTTPException(status_code=404, detail="Match not found.")
            
        # Fetch predictions
        pred_res = supabase.table("predictions").select("*").eq("match_id", match_id).execute()
        
        return {
            "match": match_res.data,
            "predictions": pred_res.data or []
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error fetching match data for %d: %s", match_id, exc)
        raise HTTPException(status_code=500, detail="Failed to fetch match data.")
