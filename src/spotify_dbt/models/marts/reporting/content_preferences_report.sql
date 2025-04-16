-- cotent_preferences_report.sql
/*
Purpose: Deliver comprehensive genre analytics
This model:
- Shows genre distribution across all listening
- Tracks genre trends over time periods
- Identifies emerging genres in user's listening
- Correlates genres with listening contexts (time of day, etc)
- Measures genre exploration vs. genre loyalty
- Ranks artists by play counts across time periods
- Calculates artist growth metrics (week-over-week changes)
- Segments artists by familiarity (new discoveries vs favorites)
- Shows diversity of genre across top artists
- Provides time-based artist ranking changes
- Source from stg_spotify_artists, stg_spotify_albums_and_tracks, stg_spotify_user_listening_history
*/

{{
    config(
        materialized='table',
        schema='reporting',
        tags=['report', 'daily']
    )
}}
