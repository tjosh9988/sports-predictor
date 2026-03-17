from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float, Text
from sqlalchemy.orm import relationship
from datetime import datetime
from app.base import Base


class SentimentScore(Base):
    """Stores sentiment analysis scores for teams from news and social media.
    Used as an additional ML feature for prediction models."""
    __tablename__ = "sentiment_scores"

    id = Column(String, primary_key=True, index=True)
    team_id = Column(String, ForeignKey("teams.id"), nullable=False, index=True)
    score = Column(Float, nullable=False)  # -1.0 (very negative) to 1.0 (very positive)
    source = Column(String, nullable=False, index=True)  # twitter, news, reddit, etc.
    summary = Column(Text, nullable=True)  # Brief summary of sentiment drivers
    recorded_at = Column(DateTime, default=datetime.utcnow, index=True)

    team = relationship("Team", back_populates="sentiments")

    def __repr__(self):
        return f"<SentimentScore(team={self.team_id}, score={self.score:.2f}, source='{self.source}')>"
