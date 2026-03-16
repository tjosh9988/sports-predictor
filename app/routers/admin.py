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

@router.get("/cleanup/fake-fixtures")
async def cleanup_fake_fixtures():
    from app.database import get_supabase_admin
    supabase = get_supabase_admin()
    result = supabase.table("matches")\
        .delete()\
        .eq("status", "upcoming")\
        .execute()
    return {"deleted": "all upcoming fixtures"}

@router.get("/cleanup/old-accumulators")
async def cleanup_old_accumulators():
    from app.database import get_supabase_admin
    from datetime import datetime, timedelta
    supabase = get_supabase_admin()
    
    # Keep only today's accumulators
    today = datetime.now().date().isoformat()
    
    # Delete legs first (foreign key)
    supabase.table("accumulator_legs")\
        .delete()\
        .lt("created_at", f"{today}T00:00:00")\
        .execute()
    
    # Delete predictions
    supabase.table("predictions")\
        .delete()\
        .lt("created_at", f"{today}T00:00:00")\
        .execute()
    
    # Delete old accumulators
    result = supabase.table("accumulators")\
        .delete()\
        .lt("created_at", f"{today}T00:00:00")\
        .execute()
    
    return {"status": "cleaned", "message": "Old accumulators removed"}

@router.get("/train/status")
async def get_training_status():
    from app.database import get_supabase_admin
    supabase = get_supabase_admin()
    try:
        result = supabase.table("model_performance")\
            .select("*")\
            .order("recorded_at", desc=True)\
            .limit(20)\
            .execute()
        
        models = result.data or []
        
        if not models:
            return {
                "status": "no_models_yet",
                "message": "Training in progress or not started",
                "models": []
            }
        
        return {
            "status": "complete",
            "models": models,
            "total_models": len(models),
            "sports_trained": list(set(
                m.get("sport") for m in models
            ))
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

@router.get("/generate/accumulators")
async def generate_accumulators(
    background_tasks: BackgroundTasks
):
    try:
        from app.services.accumulator_builder import (
            build_all_accumulators
        )
        background_tasks.add_task(build_all_accumulators)
        return {
            "status": "started",
            "message": "Building real accumulators"
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/debug/fixtures")
async def debug_fixtures():
    from app.database import get_supabase_admin
    supabase = get_supabase_admin()
    total = supabase.table("matches").select("id", count="exact").execute()
    upcoming = supabase.table("matches").select("id", count="exact").eq("status", "upcoming").execute()
    completed = supabase.table("matches").select("id", count="exact").eq("status", "completed").execute()
    sample = supabase.table("matches").select("id,sport,home_team,away_team,status,match_date").eq("status","upcoming").limit(3).execute()
    return {"total": total.count, "upcoming": upcoming.count, "completed": completed.count, "sample": sample.data}

@router.get("/create/fixtures")
async def create_fixtures(background_tasks: BackgroundTasks):
    from app.ingestion.fixture_fetcher import create_upcoming_fixtures_from_history
    background_tasks.add_task(create_upcoming_fixtures_from_history)
    return {"status": "started", "message": "Creating upcoming fixtures from historical teams"}

@router.get("/debug/api-sports")
async def debug_api_sports():
    import httpx
    import os
    api_key = os.getenv("API_SPORTS_KEY", "")
    
    if not api_key:
        return {"error": "API_SPORTS_KEY not set in environment"}
    
    results = {}
    
    # Test 1: Check API status
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://v3.football.api-sports.io/status",
                headers={"x-apisports-key": api_key}
            )
            results["api_status_code"] = resp.status_code
            results["api_response"] = resp.json()
    except Exception as e:
        results["api_error"] = str(e)
    
    # Test 2: Try fixtures
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                "https://v3.football.api-sports.io/fixtures",
                headers={"x-apisports-key": api_key},
                params={"next": 5}
            )
            data = resp.json()
            results["fixtures_status"] = resp.status_code
            results["fixtures_count"] = len(data.get("response", []))
            results["fixtures_errors"] = data.get("errors", {})
            results["api_requests_used"] = data.get("results", 0)
            if data.get("response"):
                f = data["response"][0]
                results["sample_fixture"] = {
                    "home": f.get("teams",{}).get("home",{}).get("name"),
                    "away": f.get("teams",{}).get("away",{}).get("name"),
                    "date": f.get("fixture",{}).get("date"),
                    "league": f.get("league",{}).get("name"),
                }
    except Exception as e:
        results["fixtures_error"] = str(e)
    
    results["key_preview"] = api_key[:8] + "..." if api_key else "MISSING"
    return results

@router.get("/generate/accumulators/debug")
async def generate_accumulators_debug():
    """Run accumulator generation synchronously for debugging"""
    from app.database import get_supabase_admin
    import os, numpy as np
    
    results = {}
    supabase = get_supabase_admin()
    
    # Step 1 - Check fixtures
    fixtures = supabase.table("matches")\
        .select("*")\
        .eq("status", "upcoming")\
        .limit(10)\
        .execute()
    results["fixtures_found"] = len(fixtures.data or [])
    
    if not fixtures.data:
        return {"error": "No upcoming fixtures", **results}
    
    results["first_fixture"] = fixtures.data[0]
    
    # Step 2 - Check models exist
    import glob
    models = glob.glob("/tmp/models/*.pkl")
    results["models_found"] = len(models)
    results["model_files"] = models
    
    # Step 3 - Try prediction
    try:
        from app.ml.training_pipeline import predict_match
        f = fixtures.data[0]
        pred = predict_match(
            f.get("home_team",""),
            f.get("away_team",""),
            f.get("sport","football"),
            f.get("home_odds", 2.0),
            f.get("away_odds", 2.0),
            f.get("draw_odds", 3.0)
        )
        results["prediction_test"] = pred
    except Exception as e:
        results["prediction_error"] = str(e)
    
    # Step 4 - Check accumulator builder import
    try:
        from app.services.accumulator_builder import (
            build_all_accumulators
        )
        results["builder_import"] = "success"
    except Exception as e:
        results["builder_import_error"] = str(e)
    
    return results
