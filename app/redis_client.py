"""
redis_client.py — Upstash Redis client with retry logic and helpers.

Upstash Redis is HTTP-based (REST API), so we use the `upstash-redis` SDK.
A standard `redis-py` client is also provided as a fallback for local dev
(standard Redis) or for use with Celery.

Provides:
  - `get_redis()`     : returns the Upstash REST client (preferred)
  - `get_redis_py()`  : returns a redis-py client (Celery / local dev)
  - Cache helpers     : `cache_set`, `cache_get`, `cache_delete`, `cache_exists`
"""

import json
import logging
import time
from typing import Any

from upstash_redis import Redis as UpstashRedis
import redis as redis_py

from .config import settings

logger = logging.getLogger(__name__)

# ─────────────────────── Upstash REST Client ───────────────────────────────

_upstash_client: UpstashRedis | None = None


def _init_upstash_with_retry(retries: int = 3, delay: float = 2.0) -> UpstashRedis:
    """Connect to Upstash with exponential backoff."""
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            client = UpstashRedis(
                url=settings.UPSTASH_REDIS_REST_URL,
                token=settings.UPSTASH_REDIS_REST_TOKEN,
            )
            # Verify connectivity with a PING
            pong = client.ping()
            if pong:
                logger.info("Upstash Redis connected (attempt %d).", attempt)
                return client
            raise ConnectionError("PING did not return True.")
        except Exception as exc:
            last_exc = exc
            wait = delay * (2 ** (attempt - 1))
            logger.warning(
                "Upstash Redis connection attempt %d/%d failed: %s — retrying in %.1fs",
                attempt, retries, exc, wait,
            )
            if attempt < retries:
                time.sleep(wait)
    raise ConnectionError(
        f"Could not connect to Upstash Redis after {retries} attempts."
    ) from last_exc


def get_redis() -> UpstashRedis:
    """Returns the cached Upstash REST client (preferred for production)."""
    global _upstash_client
    if _upstash_client is None:
        _upstash_client = _init_upstash_with_retry()
    return _upstash_client


# ─────────────────────── redis-py Client (Celery / local) ──────────────────

_redis_py_client: redis_py.Redis | None = None


def _init_redis_py_with_retry(retries: int = 3, delay: float = 2.0) -> redis_py.Redis:
    """Connect via redis-py (used by Celery broker or local Redis) with retry."""
    last_exc: Exception | None = None

    # Allow a plain REDIS_URL override (e.g. redis://localhost:6379)
    redis_url = settings.UPSTASH_REDIS_REST_URL  # Upstash also supports rediss:// TLS

    for attempt in range(1, retries + 1):
        try:
            client = redis_py.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
            )
            client.ping()
            logger.info("redis-py client connected (attempt %d).", attempt)
            return client
        except Exception as exc:
            last_exc = exc
            wait = delay * (2 ** (attempt - 1))
            logger.warning(
                "redis-py connection attempt %d/%d failed: %s — retrying in %.1fs",
                attempt, retries, exc, wait,
            )
            if attempt < retries:
                time.sleep(wait)
    raise ConnectionError(
        f"Could not connect to Redis via redis-py after {retries} attempts."
    ) from last_exc


def get_redis_py() -> redis_py.Redis:
    """Returns a redis-py client. Used by Celery and local dev pipelines."""
    global _redis_py_client
    if _redis_py_client is None:
        _redis_py_client = _init_redis_py_with_retry()
    return _redis_py_client


# ─────────────────────── Cache Helpers ─────────────────────────────────────

DEFAULT_TTL = 300  # 5 minutes


def cache_set(key: str, value: Any, ttl: int = DEFAULT_TTL) -> None:
    """Serialise `value` to JSON and store in Redis with a TTL (seconds)."""
    try:
        r = get_redis()
        r.setex(key, ttl, json.dumps(value, default=str))
    except Exception as exc:
        logger.error("cache_set(%s) failed: %s", key, exc)


def cache_get(key: str) -> Any | None:
    """Retrieve and deserialise a JSON value from Redis. Returns None on miss/error."""
    try:
        r = get_redis()
        raw = r.get(key)
        if raw is None:
            return None
        return json.loads(raw)
    except Exception as exc:
        logger.error("cache_get(%s) failed: %s", key, exc)
        return None


def cache_delete(key: str) -> None:
    """Delete a key from Redis."""
    try:
        get_redis().delete(key)
    except Exception as exc:
        logger.error("cache_delete(%s) failed: %s", key, exc)


def cache_exists(key: str) -> bool:
    """Returns True if the key exists in Redis."""
    try:
        return bool(get_redis().exists(key))
    except Exception as exc:
        logger.error("cache_exists(%s) failed: %s", key, exc)
        return False


def check_redis_health() -> bool:
    """Ping Upstash Redis. Returns True if healthy."""
    try:
        return bool(get_redis().ping())
    except Exception as exc:
        logger.error("Redis health check failed: %s", exc)
        return False
