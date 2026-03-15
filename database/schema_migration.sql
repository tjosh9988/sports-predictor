-- Sports Predictor Database Migration Script (REPAIR VERSION)
-- This script safely ensures all tables, columns, and indexes exist.
-- Run this in the Supabase SQL Editor.

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Create Tables (if they don't exist)
CREATE TABLE IF NOT EXISTS sports (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    slug VARCHAR(50) UNIQUE NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS leagues (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    sport_id UUID REFERENCES sports(id),
    sport VARCHAR(50),
    name VARCHAR(200) NOT NULL,
    country VARCHAR(100),
    tier INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS teams (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    sport_id UUID REFERENCES sports(id),
    sport VARCHAR(50),
    league_id UUID REFERENCES leagues(id),
    name VARCHAR(200) NOT NULL,
    short_name VARCHAR(50),
    country VARCHAR(100),
    elo_rating FLOAT DEFAULT 1500.0,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS matches (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    sport VARCHAR(50),
    sport_id UUID REFERENCES sports(id),
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

-- 2. Repair Existing Tables (Add missing columns IF table exists but column doesn't)
-- This ensures that even if tables were already there, they get the new columns.

DO $$ 
BEGIN
    -- Fix matches table
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='matches' AND column_name='sport') THEN
        ALTER TABLE matches ADD COLUMN sport VARCHAR(50);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='matches' AND column_name='sport_id') THEN
        ALTER TABLE matches ADD COLUMN sport_id UUID REFERENCES sports(id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='matches' AND column_name='league_id') THEN
        ALTER TABLE matches ADD COLUMN league_id UUID REFERENCES leagues(id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='matches' AND column_name='home_team_id') THEN
        ALTER TABLE matches ADD COLUMN home_team_id UUID REFERENCES teams(id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='matches' AND column_name='away_team_id') THEN
        ALTER TABLE matches ADD COLUMN away_team_id UUID REFERENCES teams(id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='matches' AND column_name='home_xg') THEN
        ALTER TABLE matches ADD COLUMN home_xg FLOAT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='matches' AND column_name='away_xg') THEN
        ALTER TABLE matches ADD COLUMN away_xg FLOAT;
    END IF;

    -- Fix leagues table
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='leagues' AND column_name='sport') THEN
        ALTER TABLE leagues ADD COLUMN sport VARCHAR(50);
    END IF;

    -- Fix teams table
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='teams' AND column_name='sport') THEN
        ALTER TABLE teams ADD COLUMN sport VARCHAR(50);
    END IF;
END $$;

-- 3. Create Rest of the Tables
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

CREATE TABLE IF NOT EXISTS accumulators (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    acca_type VARCHAR(10) NOT NULL,
    total_odds FLOAT,
    status VARCHAR(10) DEFAULT 'PENDING',
    ai_reasoning TEXT,
    confidence_score FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

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

CREATE TABLE IF NOT EXISTS elo_ratings (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    team_id UUID REFERENCES teams(id),
    team_name VARCHAR(200),
    sport VARCHAR(50),
    rating FLOAT DEFAULT 1500.0,
    match_id UUID REFERENCES matches(id),
    calculated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_preferences (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
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

-- 4. Create Indexes
CREATE INDEX IF NOT EXISTS idx_matches_sport ON matches(sport);
CREATE INDEX IF NOT EXISTS idx_matches_sport_id ON matches(sport_id);
CREATE INDEX IF NOT EXISTS idx_matches_date ON matches(match_date);
CREATE INDEX IF NOT EXISTS idx_matches_status ON matches(status);
CREATE INDEX IF NOT EXISTS idx_matches_sport_date ON matches(sport, match_date);
CREATE INDEX IF NOT EXISTS idx_predictions_match ON predictions(match_id);
CREATE INDEX IF NOT EXISTS idx_predictions_status ON predictions(status);
CREATE INDEX IF NOT EXISTS idx_accumulators_type ON accumulators(acca_type);
CREATE INDEX IF NOT EXISTS idx_accumulators_status ON accumulators(status);
CREATE INDEX IF NOT EXISTS idx_accumulators_date ON accumulators(created_at);

-- 5. Default Data
INSERT INTO sports (name, slug) VALUES
    ('Football', 'football'),
    ('Basketball', 'basketball'),
    ('Tennis', 'tennis'),
    ('American Football', 'nfl'),
    ('Cricket', 'cricket'),
    ('Ice Hockey', 'nhl'),
    ('Baseball', 'mlb')
ON CONFLICT (slug) DO NOTHING;
