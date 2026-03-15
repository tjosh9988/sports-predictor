from fastapi import APIRouter, BackgroundTasks
from app.database import get_supabase_admin

supabase = get_supabase_admin()

router = APIRouter(prefix="/admin", tags=["Admin"])

@router.get("/import/status")
async def import_status():
    sports = [
        "football", "tennis", "nba", 
        "nfl", "cricket", "nhl", "mlb"
    ]
    counts = {}
    total = 0
    for sport in sports:
        try:
            result = supabase.table("matches")\
                .select("id", count="exact")\
                .eq("sport", sport)\
                .execute()
            counts[sport] = result.count or 0
            total += counts[sport]
        except Exception as e:
            counts[sport] = f"error: {str(e)}"
    return {
        "status": "ok",
        "counts": counts,
        "total": total,
        "message": "Import running" if total > 0 
                   else "Database empty - run import"
    }

@router.get("/import/{sport}")
async def trigger_import(
    sport: str, 
    background_tasks: BackgroundTasks
):
    try:
        if sport == "all":
            from app.ingestion.run_importers import (
                run_all_importers
            )
            background_tasks.add_task(run_all_importers)
            return {
                "status": "started",
                "message": "All importers running"
            }
        else:
            from app.ingestion.run_importers import (
                run_single_importer
            )
            background_tasks.add_task(
                run_single_importer, sport
            )
            return {
                "status": "started",
                "sport": sport,
                "message": f"Importing {sport}"
            }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/fetch-fixtures")
async def fetch_fixtures(
    background_tasks: BackgroundTasks
):
    try:
        from app.ingestion.fixture_fetcher import (
            fetch_all_fixtures
        )
        background_tasks.add_task(fetch_all_fixtures)
        return {
            "status": "started",
            "message": "Fetching fixtures in background"
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/train/{sport}")
async def trigger_training(
    sport: str,
    background_tasks: BackgroundTasks
):
    try:
        if sport == "all":
            from app.ml.training_pipeline import (
                train_all_models
            )
            background_tasks.add_task(train_all_models)
        else:
            from app.ml.training_pipeline import (
                train_sport_models
            )
            background_tasks.add_task(
                train_sport_models, sport
            )
        return {
            "status": "started",
            "sport": sport,
            "message": f"Training {sport} models"
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/health/full")
async def full_health():
    """Check all service connections"""
    health = {}
    
    # Check Supabase
    try:
        supabase.table("matches")\
            .select("id")\
            .limit(1)\
            .execute()
        health["supabase"] = "connected"
    except Exception as e:
        health["supabase"] = f"error: {e}"
    
    # Check Redis
    try:
        from app.redis_client import redis_client
        redis_client.ping()
        health["redis"] = "connected"
    except Exception as e:
        health["redis"] = f"error: {e}"
    
    # Check API Sports key
    api_key = os.getenv("API_SPORTS_KEY", "")
    health["api_sports"] = "configured" if api_key else "missing"
    
    # Check Anthropic key
    anthro_key = os.getenv("ANTHROPIC_API_KEY", "")
    health["anthropic"] = "configured" if anthro_key else "missing"
    
    return {"status": "ok", "services": health}
