import os
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import predictions, results, sports, users
from app.config import settings

app = FastAPI(title="Sports Predictor API")

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

app.include_router(predictions.router, prefix="/predictions")
app.include_router(results.router, prefix="/results")
app.include_router(sports.router, prefix="/sports")
app.include_router(users.router, prefix="/users")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)
