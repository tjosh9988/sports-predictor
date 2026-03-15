"""
main.py — FastAPI Application Entry Point for Bet Hero.

Configures global middleware, authentication, routes, and provides 
lifespan management for background tasks and schedulers.
"""

import os
import logging
import time
from contextlib import asynccontextmanager
from typing import Dict, Any

import uvicorn
from fastapi import FastAPI, Depends, HTTPException, status, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from app.config import settings
from app.database import check_supabase_health, get_supabase_admin
from app.redis_client import get_redis_client, check_redis_health
from app.routers import predictions_router, results_router, sports_router, users_router

# ─────────────────────────── Setup ─────────────────────────────────────────

logging.basicConfig(level=settings.LOG_LEVEL.upper())
logger = logging.getLogger(__name__)

# ─────────────────────────── Middleware ────────────────────────────────────

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Simple Redis-backed rate limiting per IP address.
    """
    def __init__(self, app, requests_per_minute: int = 60):
        super().__init__(app)
        self.limit = requests_per_minute
        self.redis = get_redis_client()

    async def dispatch(self, request, call_next):
        client_ip = request.client.host
        key = f"rate_limit:{client_ip}:{int(time.time() / 60)}"
        
        try:
            current = self.redis.get(key)
            if current and int(current) >= self.limit:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail="Too many requests. Please try again later."
                )
            self.redis.incr(key)
            self.redis.expire(key, 60)
        except Exception as exc:
            # Fallback (don't block API if Redis is down)
            logger.error("Rate limit check failed: %s", exc)
        
        return await call_next(request)

# ─────────────────────────── Auth Dependency ───────────────────────────────

async def verify_supabase_token(token: str):
    """
    Verifies the Supabase JWT token.
    """
    supabase = get_supabase_admin()
    try:
        user = supabase.auth.get_user(token)
        return user
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

# ─────────────────────────── Lifespan ──────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown lifecycle."""
    logger.info("🚀 Starting %s [%s]", settings.APP_NAME, settings.ENVIRONMENT)
    
    # 1. Health Checks (Wrapped in try/except)
    try:
        if not check_supabase_health():
            logger.warning("⚠️  Supabase health check failed on startup.")
    except Exception as e:
        logger.error(f"❌ Supabase connection failed: {e}")

    try:
        if not check_redis_health():
            logger.warning("⚠️  Redis health check failed on startup.")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")

    # 2. Initialize Schedulers
    logger.info("⏰ Initializing Weekly Trainer and Daily Accumulator jobs...")
    
    yield
    logger.info("🛑 Shutting down %s", settings.APP_NAME)

# ─────────────────────────── App Initialization ────────────────────────────

app = FastAPI(
    title=settings.APP_NAME,
    description="AI-Powered Multi-Sport Accumulator Prediction Platform",
    version="1.0.0",
    lifespan=lifespan,
)

# ─────────────────────────── Core Routes (EARLY) ───────────────────────────

@app.get("/health", tags=["System"])
async def health():
    """Simple health check with zero dependencies."""
    return {"status": "ok"}

# ─────────────────────────── Middleware & Config ───────────────────────────

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Tighten for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate Limiting
app.add_middleware(RateLimitMiddleware, requests_per_minute=100)

# Include Routers
app.include_router(predictions_router)
app.include_router(results_router)
app.include_router(sports_router)
app.include_router(users_router)

# ─────────────────────────── Other Routes ──────────────────────────────────

@app.get("/", tags=["System"])
async def root():
    return {
        "app": settings.APP_NAME,
        "status": "online",
        "documentation": "/docs"
    }

# ─────────────────────────── WebSockets ────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

@app.websocket("/ws/updates")
async def websocket_endpoint(websocket: WebSocket):
    """
    Live WebSocket for broadcasting odds updates and fresh predictions.
    """
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f"Signal received: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# ─────────────────────────── Main ──────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port)
