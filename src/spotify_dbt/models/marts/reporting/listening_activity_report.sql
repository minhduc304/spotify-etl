-- listening_activity_report.sql
/*
Purpose: Provide day-level summary metrics for dashboards
This model:
- Creates daily metrics for total listening time
- Counts unique tracks and artists per day
- Shows peak listening hours
- Compares weekday vs weekend listening patterns
- Provides trend indicators (up/down vs previous day/week)
- listening_habits_report.sql
- Measures listening consistency and routines
- Calculates discovery rates for new music
- Analyzes repeat listening behavior
- Identifies listening habit changes
- Compares user patterns to platform averages
- Source from stg_spotify_user_listening_history

*/

{{
    config(
        materialized='table',
        schema='reporting',
        tags=['report', 'daily']
    )
}}

