from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from app.base import Base


class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True, index=True)
    sport = Column(String, ForeignKey("sports.slug"), nullable=False, index=True)
    league_id = Column(Integer, ForeignKey("leagues.id"), nullable=True, index=True)
    name = Column(String, nullable=False, index=True)
    short_name = Column(String)
    country = Column(String, index=True)
    elo_rating = Column(Float, default=1500.0)

    sport_rel = relationship("Sport", back_populates="teams")
    league = relationship("League", back_populates="teams")
    home_matches = relationship("Match", foreign_keys="Match.home_team_id", back_populates="home_team")
    away_matches = relationship("Match", foreign_keys="Match.away_team_id", back_populates="away_team")
    features = relationship("TeamFeature", back_populates="team")
    elo_history = relationship("EloRating", back_populates="team")
    sentiments = relationship("SentimentScore", back_populates="team")

    def __repr__(self):
        return f"<Team(id={self.id}, name='{self.name}', elo={self.elo_rating})>"
