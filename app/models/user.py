from sqlalchemy import Column, Integer, String, JSON
from app.base import Base


class UserPreference(Base):
    """Stores user preferences. Users themselves are managed by Supabase Auth;
    this table links to their Supabase auth UUID."""
    __tablename__ = "user_preferences"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, unique=True, nullable=False, index=True)  # Supabase Auth UUID
    favorite_sports = Column(JSON, default=list)  # ["football", "nba"]
    favorite_leagues = Column(JSON, default=list)  # ["premier_league", "la_liga"]
    notification_settings = Column(JSON, default=dict)  # {"push": true, "email": false, ...}

    def __repr__(self):
        return f"<UserPreference(user='{self.user_id}')>"
