ALTER SYSTEM SET max_connections = 100;
ALTER SYSTEM SET work_mem = '256MB';
ALTER SYSTEM SET maintenance_work_mem = '1GB';
ALTER SYSTEM SET effective_cache_size = '4GB';
ALTER SYSTEM SET random_page_cost = 1.1;

-- Create schemas for organizing analytical data
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS marts;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE spotify TO analyst;
GRANT ALL PRIVILEGES ON SCHEMA public TO analyst;
GRANT ALL PRIVILEGES ON SCHEMA staging TO analyst;
GRANT ALL PRIVILEGES ON SCHEMA dwh TO analyst;
GRANT ALL PRIVILEGES ON SCHEMA marts TO analyst;