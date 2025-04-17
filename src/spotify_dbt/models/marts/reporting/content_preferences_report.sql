-- cotent_preferences_report.sql
/*
Purpose: User-facing music preference reporting and insights
This model:
- Builds on artist_and_genre_preferences analytical model
- Enhances raw affinity scores with contextual dimensions
- Segments artists by familiarity and listening patterns
- Visualizes genre distribution across listening sessions
- Correlates genres with listening contexts (time of day, weekday/weekend)
- Calculates period-over-period metrics (weekly/monthly changes)
- Highlights emerging genres and trending artists
- Generates user-friendly insights for dashboard presentation

Sources from: artist_and_genre_preferences, stg_spotify_user_listening_history
*/
*/

{{
    config(
        materialized='table',
        schema='reporting',
        tags=['report', 'daily']
    )
}}
