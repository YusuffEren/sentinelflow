-- SentinelFlow PostgreSQL Initialization Script
-- This script runs on first container startup

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create schema version table for tracking migrations
CREATE TABLE IF NOT EXISTS _schema_version (
    version VARCHAR(20) PRIMARY KEY,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    description TEXT
);

-- Insert initial version marker
INSERT INTO _schema_version (version, description) 
VALUES ('0.0.0', 'Initial database creation')
ON CONFLICT (version) DO NOTHING;

-- Grant permissions (for future use with read-only replicas)
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO sentinelflow_readonly;

-- Log successful initialization
DO $$ 
BEGIN
    RAISE NOTICE 'SentinelFlow database initialized successfully';
END $$;
