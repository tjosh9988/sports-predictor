import os
import uvicorn
import asyncio
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import predictions, results, sports, users, admin
from app.config import settings
from apscheduler.schedulers.asyncio import AsyncIOScheduler

app = FastAPI(title="Sports Predictor API")
scheduler = AsyncIOScheduler()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health():
    return {"status": "ok", "version": "1.0.0"}


@app.on_event("startup")
async def startup():
    try:
        from app.database import init_db
        await init_db()
        print("Database connected")
    except Exception as e:
        print(f"Database connection failed: {e}")
    try:
        from app.redis_client import init_redis
        await init_redis()
        print("Redis connected")
    except Exception as e:
        print(f"Redis connection failed: {e}")
    
    # Start Keep-Alive task
    asyncio.create_task(keep_alive_ping())

async def keep_alive_ping():
    """Ping self every 10 minutes to prevent sleep"""
    await asyncio.sleep(60)  # Wait 1 min after startup
    while True:
        try:
            base_url = os.getenv(
                "RENDER_EXTERNAL_URL",
                "https://sports-predictor-1-o34s.onrender.com"
            )
            async with httpx.AsyncClient() as client:
                await client.get(
                    f"{base_url}/health",
                    timeout=10
                )
            print("Keep-alive ping sent")
        except Exception as e:
            print(f"Keep-alive failed: {e}")
        
        await asyncio.sleep(600)  # Ping every 10 minutes

@app.on_event("startup")
async def start_scheduler():
    scheduler.add_job(
        daily_predictions,
        'cron',
        hour=7,
        minute=0
    )
    scheduler.start()

async def daily_predictions():
    print("Running daily prediction generation...")
    from app.ingestion.fixture_fetcher import fetch_all_fixtures
    from app.services.accumulator_builder import build_all_accumulators
    await fetch_all_fixtures()
    await build_all_accumulators()
    print("Daily predictions complete")

app.include_router(predictions.router)
app.include_router(results.router)
app.include_router(sports.router)
app.include_router(users.router)
app.include_router(admin.router)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)
