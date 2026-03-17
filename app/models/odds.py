from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float
from sqlalchemy.orm import relationship
from datetime import datetime
from app.base import Base


class OddsHistory(Base):
    __tablename__ = "odds_history"

    id = Column(String, primary_key=True, index=True)
    match_id = Column(String, ForeignKey("matches.id"), nullable=False, index=True)
    bookmaker = Column(String, nullable=False, index=True)
    market = Column(String, nullable=False, index=True)  # 1X2, Over/Under 2.5, BTTS, etc.

    # Opening odds
    opening_home = Column(Float, nullable=True)
    opening_draw = Column(Float, nullable=True)
    opening_away = Column(Float, nullable=True)

    # Closing odds
    closing_home = Column(Float, nullable=True)
    closing_draw = Column(Float, nullable=True)
    closing_away = Column(Float, nullable=True)

    recorded_at = Column(DateTime, default=datetime.utcnow, index=True)

    match = relationship("Match", back_populates="odds_history")

    def __repr__(self):
        return f"<OddsHistory(match={self.match_id}, bookmaker='{self.bookmaker}', market='{self.market}')>"
