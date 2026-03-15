from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base


class Sport(Base):
    __tablename__ = "sports"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False, index=True)
    slug = Column(String, unique=True, nullable=False, index=True)

    leagues = relationship("League", back_populates="sport")
    teams = relationship("Team", back_populates="sport")
    matches = relationship("Match", back_populates="sport")
    referees = relationship("Referee", back_populates="sport")

    def __repr__(self):
        return f"<Sport(id={self.id}, name='{self.name}')>"


class League(Base):
    __tablename__ = "leagues"

    id = Column(Integer, primary_key=True, index=True)
    sport_id = Column(Integer, ForeignKey("sports.id"), nullable=False, index=True)
    name = Column(String, nullable=False, index=True)
    country = Column(String, index=True)
    tier = Column(Integer, default=1)  # 1 = top tier, 2 = second tier, etc.

    sport = relationship("Sport", back_populates="leagues")
    teams = relationship("Team", back_populates="league")
    matches = relationship("Match", back_populates="league")

    def __repr__(self):
        return f"<League(id={self.id}, name='{self.name}')>"
