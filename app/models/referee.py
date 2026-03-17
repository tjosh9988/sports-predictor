from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from app.base import Base


class Referee(Base):
    __tablename__ = "referees"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    sport = Column(String, ForeignKey("sports.slug"), nullable=False, index=True)
    avg_yellow_cards = Column(Float, default=0.0)
    avg_red_cards = Column(Float, default=0.0)
    avg_fouls = Column(Float, default=0.0)
    home_bias_score = Column(Float, default=0.0)

    sport_rel = relationship("Sport", back_populates="referees")
    matches = relationship("Match", back_populates="referee")

    def __repr__(self):
        return f"<Referee(id={self.id}, name='{self.name}')>"
