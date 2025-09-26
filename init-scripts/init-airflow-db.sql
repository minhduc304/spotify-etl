ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create additional schemas if needed
CREATE SCHEMA IF NOT EXISTS airflow_logs;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE airflow TO admin;
GRANT ALL PRIVILEGES ON SCHEMA public TO admin;
GRANT ALL PRIVILEGES ON SCHEMA airflow_logs TO admin;