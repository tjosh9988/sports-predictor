from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from app.base import Base


class Sport(Base):
    __tablename__ = "sports"

    id = Column(String, primary_key=True, index=True)
    name = Column(String, unique=True, nullable=False, index=True)
    slug = Column(String, unique=True, nullable=False, index=True)

    leagues = relationship("League", back_populates="sport_rel")
    teams = relationship("Team", back_populates="sport_rel")
    matches = relationship("Match", back_populates="sport_rel")
    referees = relationship("Referee", back_populates="sport_rel")

    def __repr__(self):
        return f"<Sport(id={self.id}, name='{self.name}')>"


class League(Base):
    __tablename__ = "leagues"

    id = Column(String, primary_key=True, index=True)
    sport = Column(String, ForeignKey("sports.slug"), nullable=False, index=True)
    name = Column(String, nullable=False, index=True)
    country = Column(String, index=True)
    tier = Column(Integer, default=1)  # 1 = top tier, 2 = second tier, etc.

    sport_rel = relationship("Sport", back_populates="leagues")
    teams = relationship("Team", back_populates="league")
    matches = relationship("Match", back_populates="league")

    def __repr__(self):
        return f"<League(id={self.id}, name='{self.name}')>"
