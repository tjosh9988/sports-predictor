-- Sports Predictor Database Migration Script
-- This script creates all necessary tables and indexes for the application.
-- Run this in the Supabase SQL Editor.

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Sports table
CREATE TABLE IF NOT EXISTS sports (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    slug VARCHAR(50) UNIQUE NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Leagues table  
CREATE TABLE IF NOT EXISTS leagues (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    sport_id UUID REFERENCES sports(id),
    sport VARCHAR(50) NOT NULL,
    name VARCHAR(200) NOT NULL,
    country VARCHAR(100),
    tier INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Teams table
CREATE TABLE IF NOT EXISTS teams (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    sport_id UUID REFERENCES sports(id),
    sport VARCHAR(50) NOT NULL,
    league_id UUID REFERENCES leagues(id),
    name VARCHAR(200) NOT NULL,
    short_name VARCHAR(50),
    country VARCHAR(100),
    elo_rating FLOAT DEFAULT 1500.0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Matches table (most important)
CREATE TABLE IF NOT EXISTS matches (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    sport VARCHAR(50) NOT NULL,
    league VARCHAR(200),
    league_id UUID REFERENCES leagues(id),
    home_team VARCHAR(200) NOT NULL,
    away_team VARCHAR(200) NOT NULL,
    home_team_id UUID REFERENCES teams(id),
    away_team_id UUID REFERENCES teams(id),
    match_date TIMESTAMP NOT NULL,
    season VARCHAR(20),
    round VARCHAR(50),
    venue VARCHAR(200),
    status VARCHAR(20) DEFAULT 'upcoming',
    home_score INTEGER,
    away_score INTEGER,
    result VARCHAR(5),
    home_odds FLOAT,
    draw_odds FLOAT,
    away_odds FLOAT,
    home_shots INTEGER,
    away_shots INTEGER,
    home_shots_target INTEGER,
    away_shots_target INTEGER,
    home_corners INTEGER,
    away_corners INTEGER,
    home_yellow_cards INTEGER,
    away_yellow_cards INTEGER,
    home_red_cards INTEGER,
    away_red_cards INTEGER,
    home_possession FLOAT,
    away_possession FLOAT,
    home_xg FLOAT,
    away_xg FLOAT,
    referee VARCHAR(200),
    attendance INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Predictions table
CREATE TABLE IF NOT EXISTS predictions (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    match_id UUID REFERENCES matches(id),
    market VARCHAR(100) NOT NULL,
    predicted_outcome VARCHAR(100),
    model_probability FLOAT,
    implied_probability FLOAT,
    edge FLOAT,
    odds FLOAT,
    confidence_score FLOAT,
    status VARCHAR(20) DEFAULT 'PENDING',
    actual_outcome VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Accumulators table
CREATE TABLE IF NOT EXISTS accumulators (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    acca_type VARCHAR(10) NOT NULL,
    total_odds FLOAT,
    status VARCHAR(10) DEFAULT 'PENDING',
    ai_reasoning TEXT,
    confidence_score FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Accumulator legs table
CREATE TABLE IF NOT EXISTS accumulator_legs (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    accumulator_id UUID REFERENCES accumulators(id),
    prediction_id UUID REFERENCES predictions(id),
    match_id UUID REFERENCES matches(id),
    sport VARCHAR(50),
    league VARCHAR(200),
    home_team VARCHAR(200),
    away_team VARCHAR(200),
    market VARCHAR(100),
    predicted_outcome VARCHAR(100),
    odds FLOAT,
    confidence FLOAT,
    edge FLOAT,
    ai_reasoning TEXT,
    status VARCHAR(20) DEFAULT 'PENDING',
    actual_outcome VARCHAR(100),
    leg_order INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Model performance table
CREATE TABLE IF NOT EXISTS model_performance (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    model_name VARCHAR(100),
    sport VARCHAR(50),
    market VARCHAR(100),
    accuracy FLOAT,
    roi FLOAT,
    win_rate FLOAT,
    total_predictions INTEGER DEFAULT 0,
    recorded_at TIMESTAMP DEFAULT NOW()
);

-- Sentiment scores table
CREATE TABLE IF NOT EXISTS sentiment_scores (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    team_id UUID REFERENCES teams(id),
    team_name VARCHAR(200),
    sport VARCHAR(50),
    score FLOAT,
    source VARCHAR(100),
    summary TEXT,
    recorded_at TIMESTAMP DEFAULT NOW()
);

-- Elo ratings table
CREATE TABLE IF NOT EXISTS elo_ratings (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    team_id UUID REFERENCES teams(id),
    team_name VARCHAR(200),
    sport VARCHAR(50),
    rating FLOAT DEFAULT 1500.0,
    match_id UUID REFERENCES matches(id),
    calculated_at TIMESTAMP DEFAULT NOW()
);

-- User preferences table
CREATE TABLE IF NOT EXISTS user_preferences (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id),
    favorite_sports TEXT[],
    favorite_leagues TEXT[],
    default_stake FLOAT DEFAULT 10.0,
    odds_format VARCHAR(20) DEFAULT 'decimal',
    notification_new_acca BOOLEAN DEFAULT true,
    notification_results BOOLEAN DEFAULT true,
    notification_odds_movement BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_matches_sport 
    ON matches(sport);
CREATE INDEX IF NOT EXISTS idx_matches_date 
    ON matches(match_date);
CREATE INDEX IF NOT EXISTS idx_matches_status 
    ON matches(status);
CREATE INDEX IF NOT EXISTS idx_matches_sport_date 
    ON matches(sport, match_date);
CREATE INDEX IF NOT EXISTS idx_predictions_match 
    ON predictions(match_id);
CREATE INDEX IF NOT EXISTS idx_predictions_status 
    ON predictions(status);
CREATE INDEX IF NOT EXISTS idx_accumulators_type 
    ON accumulators(acca_type);
CREATE INDEX IF NOT EXISTS idx_accumulators_status 
    ON accumulators(status);
CREATE INDEX IF NOT EXISTS idx_accumulators_date 
    ON accumulators(created_at);

-- Insert default sports
INSERT INTO sports (name, slug) VALUES
    ('Football', 'football'),
    ('Basketball', 'basketball'),
    ('Tennis', 'tennis'),
    ('American Football', 'nfl'),
    ('Cricket', 'cricket'),
    ('Ice Hockey', 'nhl'),
    ('Baseball', 'mlb')
ON CONFLICT (slug) DO NOTHING;
