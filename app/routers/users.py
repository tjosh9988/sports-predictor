"""
users.py — FastAPI router for user preferences and notifications.
"""

from __future__ import annotations

import logging
from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException
from ..database import get_supabase_admin
from ..schemas.schemas import UserPreferenceOut, UserPreferenceBase

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/users",
    tags=["Users"]
)

# ─────────────────────────── Mock Auth ─────────────────────────────────────

def get_current_user_id() -> str:
    """
    Dependency to get the current authenticated user ID from Supabase JWT.
    In a real implementation, this would parse the Authorization header via a verification utility.
    """
    # Placeholder for auth logic
    return "test-user-id"

# ─────────────────────────── Router ────────────────────────────────────────

@router.get("/preferences", response_model=UserPreferenceOut)
async def get_preferences(user_id: str = Depends(get_current_user_id)):
    """
    Fetch preferences for the current authenticated user.
    """
    supabase = get_supabase_admin()
    try:
        res = (
            supabase.table("user_preferences")
            .select("*")
            .eq("user_id", user_id)
            .single()
            .execute()
        )
        if not res.data:
            # Create default preferences if not found
            res = supabase.table("user_preferences").insert({"user_id": user_id}).execute()
            return res.data[0]
            
        return res.data
    except Exception as exc:
        logger.error("Error fetching preferences for %s: %s", user_id, exc)
        raise HTTPException(status_code=500, detail="Failed to fetch preferences.")

@router.put("/preferences", response_model=UserPreferenceOut)
async def update_preferences(
    pref: UserPreferenceBase, 
    user_id: str = Depends(get_current_user_id)
):
    """
    Update preferences for the current authenticated user.
    """
    supabase = get_supabase_admin()
    try:
        res = (
            supabase.table("user_preferences")
            .update(pref.model_dump())
            .eq("user_id", user_id)
            .execute()
        )
        if not res.data:
            raise HTTPException(status_code=404, detail="Preferences record not found.")
            
        return res.data[0]
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error updating preferences for %s: %s", user_id, exc)
        raise HTTPException(status_code=500, detail="Failed to update preferences.")

@router.post("/notifications/subscribe", response_model=Dict[str, str])
async def subscribe_to_notifications(
    settings: Dict[str, Any], 
    user_id: str = Depends(get_current_user_id)
):
    """
    Subscribe the current user to specific notification triggers.
    """
    supabase = get_supabase_admin()
    try:
        # Update the notification_settings JSON field in user_preferences
        res = (
            supabase.table("user_preferences")
            .update({"notification_settings": settings})
            .eq("user_id", user_id)
            .execute()
        )
        return {"status": "success", "message": "Notification preferences updated."}
    except Exception as exc:
        logger.error("Error subscribing user %s: %s", user_id, exc)
        raise HTTPException(status_code=500, detail="Failed to update subscription.")
