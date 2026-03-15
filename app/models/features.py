from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float
from sqlalchemy.orm import relationship
from datetime import datetime
from app.base import Base


class TeamFeature(Base):
    """Pre-computed features for ML models. All features are calculated
    using only data available BEFORE the match (no data leakage)."""
    __tablename__ = "team_features"

    id = Column(Integer, primary_key=True, index=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    calculated_at = Column(DateTime, default=datetime.utcnow, index=True)
    feature_name = Column(String, nullable=False, index=True)  # form_last5, avg_goals_scored, etc.
    feature_value = Column(Float, nullable=False)

    team = relationship("Team", back_populates="features")

    def __repr__(self):
        return f"<TeamFeature(team={self.team_id}, '{self.feature_name}'={self.feature_value})>"


class EloRating(Base):
    """Point-in-time ELO rating snapshots, tied to the match that
    triggered the recalculation."""
    __tablename__ = "elo_ratings"

    id = Column(Integer, primary_key=True, index=True)
    team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    rating = Column(Float, nullable=False)
    calculated_at = Column(DateTime, default=datetime.utcnow, index=True)
    match_id = Column(Integer, ForeignKey("matches.id"), nullable=True, index=True)

    team = relationship("Team", back_populates="elo_history")
    match = relationship("Match", back_populates="elo_snapshots")

    def __repr__(self):
        return f"<EloRating(team={self.team_id}, rating={self.rating})>"
