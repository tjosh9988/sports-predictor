from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import Base


class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, index=True)
    match_id = Column(Integer, ForeignKey("matches.id"), nullable=False, index=True)
    market = Column(String, nullable=False, index=True)  # 1X2, Over/Under 2.5, BTTS, etc.
    predicted_outcome = Column(String, nullable=False)  # Home, Away, Draw, Over, Under, Yes, No
    model_probability = Column(Float, nullable=False)  # ML model output probability
    implied_probability = Column(Float, nullable=False)  # Implied from bookmaker odds
    edge = Column(Float, nullable=False)  # model_probability - implied_probability
    odds = Column(Float, nullable=False)  # Decimal odds for this outcome
    confidence_score = Column(Float, nullable=False)  # Ensemble confidence 0-1
    status = Column(String, default="PENDING", index=True)  # PENDING, CORRECT, INCORRECT
    actual_outcome = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    match = relationship("Match", back_populates="predictions")
    result = relationship("PredictionResult", back_populates="prediction", uselist=False)
    accumulator_legs = relationship("AccumulatorLeg", back_populates="prediction")

    def __repr__(self):
        return f"<Prediction(match={self.match_id}, market='{self.market}', edge={self.edge:.3f})>"


class PredictionResult(Base):
    __tablename__ = "prediction_results"

    id = Column(Integer, primary_key=True, index=True)
    prediction_id = Column(Integer, ForeignKey("predictions.id"), nullable=False, unique=True, index=True)
    actual_result = Column(String, nullable=False)
    goals_home = Column(Integer, nullable=True)
    goals_away = Column(Integer, nullable=True)
    resolved_at = Column(DateTime, default=datetime.utcnow)

    prediction = relationship("Prediction", back_populates="result")

    def __repr__(self):
        return f"<PredictionResult(prediction={self.prediction_id}, result='{self.actual_result}')>"
