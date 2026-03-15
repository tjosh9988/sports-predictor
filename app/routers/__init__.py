# app/routers/__init__.py
from .predictions import router as predictions_router
from .results import router as results_router
from .sports import router as sports_router
from .users import router as users_router

__all__ = [
    "predictions_router",
    "results_router",
    "sports_router",
    "users_router",
]
