from sqlalchemy import Column, Integer, String, DateTime, Float
from datetime import datetime
from app.base import Base


class ModelPerformance(Base):
    """Tracks the historical performance of each ML model per sport and market.
    Updated periodically after match results are resolved."""
    __tablename__ = "model_performance"

    id = Column(Integer, primary_key=True, index=True)
    model_name = Column(String, nullable=False, index=True)  # random_forest, xgboost, lightgbm, lstm
    sport = Column(String, nullable=False, index=True)
    market = Column(String, nullable=False, index=True)  # 1X2, Over/Under 2.5, etc.
    accuracy = Column(Float, nullable=False)
    roi = Column(Float, nullable=False)  # Return on Investment %
    win_rate = Column(Float, nullable=False)
    total_predictions = Column(Integer, nullable=False)
    recorded_at = Column(DateTime, default=datetime.utcnow, index=True)

    def __repr__(self):
        return f"<ModelPerformance(model='{self.model_name}', sport='{self.sport}', accuracy={self.accuracy:.2f})>"
