"""
results.py — FastAPI router for historical results and model performance.
"""

from __future__ import annotations

import logging
from typing import List, Optional, Dict
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query
from app.database import get_supabase_admin
from app.schemas.schemas import PredictionOut, ModelPerformanceOut

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/results",
    tags=["Results"]
)

@router.get("/history", response_model=List[PredictionOut])
async def get_results_history(limit: int = 50, sport: Optional[str] = None):
    """
    Fetch history of resolved predictions.
    """
    supabase = get_supabase_admin()
    try:
        query = supabase.table("predictions").select("*").eq("status", "resolved").order("created_at", desc=True)
        
        if sport:
            # We'd need to join with matches/sports to filter by sport slug
            # For simplicity in this implementation, we assume predictions are filtered by ID list if needed
            pass
            
        res = query.limit(limit).execute()
        return res.data or []
    except Exception as exc:
        logger.error("Error fetching results history: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to fetch history.")

@router.get("/performance", response_model=List[ModelPerformanceOut])
async def get_overall_performance():
    """
    Fetch the latest performance metrics for all models.
    """
    supabase = get_supabase_admin()
    try:
        res = (
            supabase.table("model_performance")
            .select("*")
            .order("recorded_at", desc=True)
            .execute()
        )
        return res.data or []
    except Exception as exc:
        logger.error("Error fetching model performance: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to fetch performance data.")

@router.get("/accuracy/{sport}", response_model=Dict[str, float])
async def get_sport_accuracy(sport: str):
    """
    Fetch accuracy breakdown by market for a specific sport.
    """
    supabase = get_supabase_admin()
    try:
        res = (
            supabase.table("model_performance")
            .select("market, accuracy")
            .eq("sport", sport)
            .order("recorded_at", desc=True)
            .execute()
        )
        if not res.data:
            return {}
        
        # Flatten into market: accuracy map
        return {item["market"]: item["accuracy"] for item in res.data}
    except Exception as exc:
        logger.error("Error fetching accuracy for %s: %s", sport, exc)
        raise HTTPException(status_code=500, detail="Failed to fetch accuracy data.")

@router.get("/roi/{timeframe}", response_model=Dict[str, float])
async def get_roi_stats(timeframe: str = "30d"):
    """
    Fetch ROI stats for different timeframes (e.g., 7d, 30d, 90d, all).
    """
    # In a real implementation, we would aggregate resolved predictions 
    # and accumulators to calculate real ROI.
    # Here we simulate with the latest recorded values.
    supabase = get_supabase_admin()
    try:
        res = supabase.table("model_performance").select("roi").execute()
        rois = [r["roi"] for r in res.data or [] if r.get("roi")]
        
        avg_roi = sum(rois) / len(rois) if rois else 0.0
        
        return {
            "timeframe": timeframe,
            "overall_roi": round(avg_roi, 4)
        }
    except Exception as exc:
        logger.error("Error fetching ROI stats: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to fetch ROI data.")
