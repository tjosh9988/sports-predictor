"""
predictions.py — FastAPI router for match predictions and accumulators.
"""

from __future__ import annotations

import logging
from typing import List, Optional
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from app.database import get_supabase_admin
from app.schemas.schemas import PredictionOut, AccumulatorOut, MatchOut

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/predictions",
    tags=["Predictions"]
)

@router.get("/accumulators/today")
async def get_today_accumulators():
    """
    Fetch all accumulators generated in the last 24 hours.
    Returns mock data if empty.
    """
    supabase = get_supabase_admin()
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
            return {"data": res.data, "count": len(res.data), "source": "database"}
            
        # Mock data if empty
        return {
            "data": [
                {
                    "id": 5001,
                    "acca_type": "10odds",
                    "total_odds": 10.24,
                    "status": "pending",
                    "confidence_score": 67,
                    "ai_reasoning": "Selected based on strong home form, value odds detected across 5 leagues",
                    "created_at": now.isoformat(),
                    "legs": [
                        {
                            "id": 6001,
                            "accumulator_id": 5001,
                            "prediction_id": 7001,
                            "leg_order": 1,
                            "odds": 2.10,
                            "status": "pending"
                        },
                        {
                            "id": 6002,
                            "accumulator_id": 5001,
                            "prediction_id": 7002,
                            "leg_order": 2,
                            "odds": 1.85,
                            "status": "pending"
                        },
                        {
                            "id": 6003,
                            "accumulator_id": 5001,
                            "prediction_id": 7003,
                            "leg_order": 3,
                            "odds": 1.72,
                            "status": "pending"
                        }
                    ]
                }
            ],
            "count": 1,
            "source": "sample"
        }
    except Exception as exc:
        logger.error("Error fetching today's accumulators: %s", exc)
        return {"data": [], "count": 0, "error": str(exc)}

@router.get("/accumulators/{acca_type}", response_model=List[AccumulatorOut])
async def get_accumulators_by_type(acca_type: str):
    """
    Fetch all accumulators of a specific type (3odds, 5odds, 10odds).
    """
    if acca_type not in ["3odds", "5odds", "10odds"]:
        raise HTTPException(status_code=400, detail="Invalid accumulator type.")
        
    supabase = get_supabase_admin()
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
        return {"data": data, "count": len(data)}
    except Exception as exc:
        logger.error("Error fetching %s accumulators: %s", acca_type, exc)
        raise HTTPException(status_code=500, detail=f"Failed to fetch {acca_type} accumulators.")

@router.get("/fixtures/{sport}", response_model=List[MatchOut])
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

@router.get("/match/{match_id}", response_model=dict)
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
