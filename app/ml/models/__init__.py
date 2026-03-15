# ml/models/__init__.py
from .base_model import BetHeroBaseModel, TrainingResult, BacktestResult, PredictionBatch
from .random_forest_model import RandomForestBetModel
from .xgboost_model import XGBoostBetModel
from .lightgbm_model import LightGBMBetModel
from .neural_network_model import NeuralNetworkBetModel
from .lstm_model import LSTMBetModel
from .ensemble import StackingEnsemble, SportWeightRegistry, WeightedStackBuilder

# Global Constants
SUPPORTED_SPORTS: list[str] = [
    "football", "nba", "nfl", "tennis", "cricket", "nhl", "mlb",
]

MARKETS: list[str] = [
    "match_result",
    "btts",
    "over_25",
    "over_35",
    "asian_handicap",
    "cards_over",
    "corners_over",
]

__all__ = [
    # Base
    "BetHeroBaseModel",
    "TrainingResult",
    "BacktestResult",
    "PredictionBatch",
    # Concrete Models
    "RandomForestBetModel",
    "XGBoostBetModel",
    "LightGBMBetModel",
    "NeuralNetworkBetModel",
    "LSTMBetModel",
    # Ensemble
    "StackingEnsemble",
    "SportWeightRegistry",
    "WeightedStackBuilder",
    # Constants
    "SUPPORTED_SPORTS",
    "MARKETS",
]
