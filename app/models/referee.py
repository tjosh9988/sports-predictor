from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base


class Referee(Base):
    __tablename__ = "referees"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    sport_id = Column(Integer, ForeignKey("sports.id"), nullable=False, index=True)
    avg_yellow_cards = Column(Float, default=0.0)
    avg_red_cards = Column(Float, default=0.0)
    avg_fouls = Column(Float, default=0.0)
    home_bias_score = Column(Float, default=0.0)

    sport = relationship("Sport", back_populates="referees")
    matches = relationship("Match", back_populates="referee")

    def __repr__(self):
        return f"<Referee(id={self.id}, name='{self.name}')>"
