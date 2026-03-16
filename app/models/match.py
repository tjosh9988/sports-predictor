from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float
from sqlalchemy.orm import relationship
from app.base import Base


class Match(Base):
    __tablename__ = "matches"

    id = Column(Integer, primary_key=True, index=True)
    sport = Column(String, ForeignKey("sports.slug"), nullable=False, index=True)
    league_id = Column(Integer, ForeignKey("leagues.id"), nullable=False, index=True)
    home_team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    away_team_id = Column(Integer, ForeignKey("teams.id"), nullable=False, index=True)
    match_date = Column(DateTime, nullable=False, index=True)
    status = Column(String, default="upcoming", index=True)  # upcoming, live, finished, postponed
    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)
    season = Column(String, index=True)
    round = Column(String, nullable=True)
    venue = Column(String, nullable=True)
    referee_id = Column(Integer, ForeignKey("referees.id"), nullable=True)
    attendance = Column(Integer, nullable=True)

    sport_rel = relationship("Sport", back_populates="matches")
    league = relationship("League", back_populates="matches")
    home_team = relationship("Team", foreign_keys=[home_team_id], back_populates="home_matches")
    away_team = relationship("Team", foreign_keys=[away_team_id], back_populates="away_matches")
    referee = relationship("Referee", back_populates="matches")
    stats = relationship("MatchStat", back_populates="match", cascade="all, delete-orphan")
    odds_history = relationship("OddsHistory", back_populates="match", cascade="all, delete-orphan")
    predictions = relationship("Prediction", back_populates="match")
    elo_snapshots = relationship("EloRating", back_populates="match")

    def __repr__(self):
        return f"<Match(id={self.id}, {self.home_team_id} vs {self.away_team_id}, {self.match_date})>"


class MatchStat(Base):
    __tablename__ = "match_stats"

    id = Column(Integer, primary_key=True, index=True)
    match_id = Column(Integer, ForeignKey("matches.id"), nullable=False, index=True)
    stat_type = Column(String, nullable=False, index=True)  # possession, shots, corners, etc.
    home_value = Column(Float)
    away_value = Column(Float)

    match = relationship("Match", back_populates="stats")

    def __repr__(self):
        return f"<MatchStat(match={self.match_id}, type='{self.stat_type}')>"
