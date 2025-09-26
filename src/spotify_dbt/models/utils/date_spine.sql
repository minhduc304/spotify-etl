-- utils/date_spine.sql
/*
Purpose: Generate a complete date dimension table
This model:
- Creates a continuous series of dates for time-based analysis
- Adds calendar attributes (day of week, month, quarter, etc)
- Provides consistent date handling across all models
*/

{{
    config(
        materialized='view'
    )
}}

-- Placeholder implementation - generates a simple date spine for the last 30 days
SELECT 
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(dow from date_day) as day_of_week
FROM generate_series(
    current_date - interval '30 days',
    current_date,
    interval '1 day'
) as date_day