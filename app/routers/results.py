from __future__ import annotations
import logging
from typing import List, Optional, Dict
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from app.database import get_supabase_admin

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/results",
    tags=["Results"]
)

@router.get("/history")
async def get_results_history():
    from app.database import get_supabase_admin
    supabase = get_supabase_admin()
    try:
        result = supabase.table("accumulators")\
            .select("*, legs:accumulator_legs(*)")\
            .order("created_at", desc=True)\
            .limit(50)\
            .execute()
        return {
            "data": result.data or [],
            "count": len(result.data or [])
        }
    except Exception as e:
        return {"data": [], "count": 0, "error": str(e)}

@router.get("/performance")
async def get_overall_performance():
    from app.database import get_supabase_admin
    supabase = get_supabase_admin()
    try:
        accas = supabase.table("accumulators")\
            .select("acca_type, status, total_odds")\
            .execute()
        data = accas.data or []
        total = len(data)
        won = sum(1 for a in data if a.get("status") == "WON")
        lost = sum(1 for a in data if a.get("status") == "LOST")
        pending = sum(1 for a in data if a.get("status") == "PENDING")
        win_rate = round(won / total * 100, 1) if total > 0 else 0

        by_type = {}
        for t in ["10odds", "5odds", "3odds"]:
            type_data = [
                a for a in data if a.get("acca_type") == t
            ]
            type_won = sum(
                1 for a in type_data if a.get("status") == "WON"
            )
            by_type[t] = {
                "total": len(type_data),
                "won": type_won,
                "win_rate": round(
                    type_won / len(type_data) * 100, 1
                ) if type_data else 0
            }

        model_perf = supabase.table("model_performance")\
            .select("*")\
            .order("recorded_at", desc=True)\
            .limit(10)\
            .execute()

        return {
            "total_predictions": total,
            "won": won,
            "lost": lost,
            "pending": pending,
            "win_rate": win_rate,
            "by_type": by_type,
            "models": model_perf.data or []
        }
    except Exception as e:
        return {"error": str(e)}
