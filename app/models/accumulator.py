from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, Text
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import Base


class Accumulator(Base):
    __tablename__ = "accumulators"

    id = Column(Integer, primary_key=True, index=True)
    acca_type = Column(String, nullable=False, index=True)  # 3odds, 5odds, 10odds
    total_odds = Column(Float, nullable=False)
    status = Column(String, default="PENDING", index=True)  # PENDING, WON, LOST
    ai_reasoning = Column(Text, nullable=True)  # AI explanation of why these legs were chosen
    confidence_score = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    legs = relationship("AccumulatorLeg", back_populates="accumulator", cascade="all, delete-orphan",
                        order_by="AccumulatorLeg.leg_order")

    def __repr__(self):
        return f"<Accumulator(id={self.id}, type='{self.acca_type}', odds={self.total_odds})>"


class AccumulatorLeg(Base):
    __tablename__ = "accumulator_legs"

    id = Column(Integer, primary_key=True, index=True)
    accumulator_id = Column(Integer, ForeignKey("accumulators.id"), nullable=False, index=True)
    prediction_id = Column(Integer, ForeignKey("predictions.id"), nullable=False, index=True)
    leg_order = Column(Integer, nullable=False)
    odds = Column(Float, nullable=False)
    status = Column(String, default="PENDING")  # PENDING, WON, LOST

    accumulator = relationship("Accumulator", back_populates="legs")
    prediction = relationship("Prediction", back_populates="accumulator_legs")

    def __repr__(self):
        return f"<AccumulatorLeg(acca={self.accumulator_id}, leg={self.leg_order}, odds={self.odds})>"
