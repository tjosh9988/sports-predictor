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

@router.get("/debug/storage/{sport}")
async def debug_storage(sport: str):
    from app.database import get_supabase_admin
    supabase = get_supabase_admin()
    
    results = {}
    
    # Test 1: List files in bucket
    try:
        files = supabase.storage\
            .from_("sports-data")\
            .list(sport)
        results["files_found"] = len(files) if files else 0
        results["file_names"] = [
            f.get("name") for f in (files or [])
        ][:5]  # First 5 only
    except Exception as e:
        results["storage_error"] = str(e)
    
    # Test 2: Try downloading first file
    if results.get("files_found", 0) > 0:
        first_file = results["file_names"][0]
        try:
            data = supabase.storage\
                .from_("sports-data")\
                .download(f"{sport}/{first_file}")
            results["download_size"] = len(data)
            results["download_status"] = "success"
            
            # Test 3: Try reading as CSV
            import io
            import pandas as pd
            df = pd.read_csv(
                io.BytesIO(data), 
                low_memory=False,
                nrows=5
            )
            results["csv_columns"] = list(df.columns)
            results["csv_rows_sample"] = 5
            results["csv_status"] = "readable"
            
        except Exception as e:
            results["download_error"] = str(e)
    
    # Test 4: Test database insert
    try:
        from app.database import get_supabase_admin
        db = get_supabase_admin()
        test = db.table("matches")\
            .select("id")\
            .limit(1)\
            .execute()
        results["db_status"] = "connected"
        results["db_matches_count"] = len(test.data)
    except Exception as e:
        results["db_error"] = str(e)
    
    return results

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
        from app.redis_client import get_redis
        r = get_redis()
        r.ping()
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
