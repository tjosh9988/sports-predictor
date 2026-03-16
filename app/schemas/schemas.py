from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Optional


# ───────────────────────── Sport & League ─────────────────────────

class SportBase(BaseModel):
    name: str
    slug: str

class SportCreate(SportBase):
    pass

class SportOut(SportBase):
    id: int
    class Config:
        from_attributes = True


class LeagueBase(BaseModel):
    name: str
    country: Optional[str] = None
    tier: int = 1

class LeagueCreate(LeagueBase):
    sport: str

class LeagueOut(LeagueBase):
    id: int
    sport: str
    class Config:
        from_attributes = True


# ─────────────────────────── Team ─────────────────────────────────

class TeamBase(BaseModel):
    name: str
    short_name: Optional[str] = None
    country: Optional[str] = None
    elo_rating: float = 1500.0

class TeamCreate(TeamBase):
    sport: str
    league_id: Optional[int] = None

class TeamOut(TeamBase):
    id: int
    sport: str
    league_id: Optional[int] = None
    class Config:
        from_attributes = True


# ─────────────────────────── Referee ──────────────────────────────

class RefereeBase(BaseModel):
    name: str
    avg_yellow_cards: float = 0.0
    avg_red_cards: float = 0.0
    avg_fouls: float = 0.0
    home_bias_score: float = 0.0

class RefereeCreate(RefereeBase):
    sport: str

class RefereeOut(RefereeBase):
    id: int
    sport: str
    class Config:
        from_attributes = True


# ─────────────────────────── Match ────────────────────────────────

class MatchBase(BaseModel):
    home_team_id: int
    away_team_id: int
    match_date: datetime
    status: str = "upcoming"
    season: Optional[str] = None
    round: Optional[str] = None
    venue: Optional[str] = None
    attendance: Optional[int] = None

class MatchCreate(MatchBase):
    sport: str
    league_id: int
    referee_id: Optional[int] = None

class MatchOut(MatchBase):
    id: int
    sport: str
    league_id: int
    referee_id: Optional[int] = None
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    class Config:
        from_attributes = True


class MatchStatBase(BaseModel):
    stat_type: str
    home_value: float
    away_value: float

class MatchStatCreate(MatchStatBase):
    match_id: int

class MatchStatOut(MatchStatBase):
    id: int
    match_id: int
    class Config:
        from_attributes = True


# ─────────────────────── Odds History ─────────────────────────────

class OddsHistoryBase(BaseModel):
    bookmaker: str
    market: str
    opening_home: Optional[float] = None
    opening_draw: Optional[float] = None
    opening_away: Optional[float] = None
    closing_home: Optional[float] = None
    closing_draw: Optional[float] = None
    closing_away: Optional[float] = None

class OddsHistoryCreate(OddsHistoryBase):
    match_id: int

class OddsHistoryOut(OddsHistoryBase):
    id: int
    match_id: int
    recorded_at: datetime
    class Config:
        from_attributes = True


# ────────────────── Team Features & ELO ───────────────────────────

class TeamFeatureBase(BaseModel):
    feature_name: str
    feature_value: float

class TeamFeatureCreate(TeamFeatureBase):
    team_id: int

class TeamFeatureOut(TeamFeatureBase):
    id: int
    team_id: int
    calculated_at: datetime
    class Config:
        from_attributes = True


class EloRatingBase(BaseModel):
    rating: float

class EloRatingCreate(EloRatingBase):
    team_id: int
    match_id: Optional[int] = None

class EloRatingOut(EloRatingBase):
    id: int
    team_id: int
    match_id: Optional[int] = None
    calculated_at: datetime
    class Config:
        from_attributes = True


# ──────────────────── Predictions ─────────────────────────────────

class PredictionBase(BaseModel):
    market: str
    predicted_outcome: str
    model_probability: float
    implied_probability: float
    edge: float
    odds: float
    confidence_score: float

class PredictionCreate(PredictionBase):
    match_id: int

class PredictionOut(PredictionBase):
    id: int
    match_id: int
    status: str
    actual_outcome: Optional[str] = None
    created_at: datetime
    class Config:
        from_attributes = True


class PredictionResultBase(BaseModel):
    actual_result: str
    goals_home: Optional[int] = None
    goals_away: Optional[int] = None

class PredictionResultCreate(PredictionResultBase):
    prediction_id: int

class PredictionResultOut(PredictionResultBase):
    id: int
    prediction_id: int
    resolved_at: datetime
    class Config:
        from_attributes = True


# ──────────────────── Accumulators ────────────────────────────────

class AccumulatorLegOut(BaseModel):
    id: int
    prediction_id: int
    leg_order: int
    odds: float
    status: str
    class Config:
        from_attributes = True


class AccumulatorBase(BaseModel):
    acca_type: str  # 3odds, 5odds, 10odds
    total_odds: float
    confidence_score: float
    ai_reasoning: Optional[str] = None

class AccumulatorCreate(AccumulatorBase):
    leg_prediction_ids: List[int]  # List of prediction IDs for the legs

class AccumulatorOut(AccumulatorBase):
    id: int
    status: str
    created_at: datetime
    legs: List[AccumulatorLegOut] = []
    class Config:
        from_attributes = True


# ────────────────── User Preferences ─────────────────────────────

class UserPreferenceBase(BaseModel):
    favorite_sports: List[str] = []
    favorite_leagues: List[str] = []
    notification_settings: dict = {}

class UserPreferenceCreate(UserPreferenceBase):
    user_id: str  # Supabase Auth UUID

class UserPreferenceOut(UserPreferenceBase):
    id: int
    user_id: str
    class Config:
        from_attributes = True


# ───────────────── Model Performance ─────────────────────────────

class ModelPerformanceBase(BaseModel):
    model_name: str
    sport: str
    market: str
    accuracy: float
    roi: float
    win_rate: float
    total_predictions: int

class ModelPerformanceOut(ModelPerformanceBase):
    id: int
    recorded_at: datetime
    class Config:
        from_attributes = True


# ──────────────── Sentiment Scores ───────────────────────────────

class SentimentScoreBase(BaseModel):
    score: float = Field(..., ge=-1.0, le=1.0)
    source: str
    summary: Optional[str] = None

class SentimentScoreCreate(SentimentScoreBase):
    team_id: int

class SentimentScoreOut(SentimentScoreBase):
    id: int
    team_id: int
    recorded_at: datetime
    class Config:
        from_attributes = True
