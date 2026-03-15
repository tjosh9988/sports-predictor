"""
main.py — FastAPI Application Entry Point for Bet Hero.

Configures global middleware, authentication, routes, and provides 
lifespan management for background tasks and schedulers.
"""

import logging
import time
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, Depends, HTTPException, status, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from .config import settings
from .database import check_supabase_health, get_supabase_admin
from .redis_client import get_redis_client, check_redis_health
from .routers import predictions_router, results_router, sports_router, users_router

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
    In reality, we use supabase-py or a JWT library to decode and check the 'sub' claim.
    """
    supabase = get_supabase_admin()
    try:
        # Simplification: call supabase.auth.get_user(token)
        # This confirms the token is valid with the Supabase auth server.
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
    
    # 1. Health Checks
    if not check_supabase_health():
        logger.warning("⚠️  Supabase health check failed on startup.")
    if not check_redis_health():
        logger.warning("⚠️  Redis health check failed on startup.")

    # 2. Initialize Schedulers
    # In a real microservice, these might run as separate worker processes.
    # Here we simulate starting the specialized background jobs.
    logger.info("⏰ Initializing Weekly Trainer and Daily Accumulator jobs...")
    # from .ml.training_pipeline import start_scheduler
    # from .ml.accumulator_builder import run_accumulator_job
    # asyncio.create_task(run_scheduler_in_background())
    
    yield
    logger.info("🛑 Shutting down %s", settings.APP_NAME)

# ─────────────────────────── App Initialization ────────────────────────────

app = FastAPI(
    title=settings.APP_NAME,
    description="AI-Powered Multi-Sport Accumulator Prediction Platform",
    version="1.0.0",
    lifespan=lifespan,
)

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

# ─────────────────────────── Core Routes ───────────────────────────────────

@app.get("/", tags=["System"])
async def root():
    return {
        "app": settings.APP_NAME,
        "status": "online",
        "documentation": "/docs"
    }

@app.get("/health", tags=["System"])
async def health():
    """Full health check — confirms Supabase and Redis connectivity."""
    s_ok = check_supabase_health()
    r_ok = check_redis_health()
    return {
        "status": "healthy" if (s_ok and r_ok) else "degraded",
        "services": {"supabase": s_ok, "redis": r_ok}
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
            # Wait for any incoming messages (or just keep connection open)
            data = await websocket.receive_text()
            # Echo or handle incoming client signal
            await websocket.send_text(f"Signal received: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
