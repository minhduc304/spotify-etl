-- genre_breakdown_report.sql
/*
Purpose: Deliver comprehensive genre analytics
This model:
- Shows genre distribution across all listening
- Tracks genre trends over time periods
- Identifies emerging genres in user's listening
- Correlates genres with listening contexts (time of day, etc)
- Measures genre exploration vs. genre loyalty

-- top_artists_report.sql
Purpose: Deliver insights on most-played artists
This model:
- Ranks artists by play counts across time periods
- Calculates artist growth metrics (week-over-week changes)
- Segments artists by familiarity (new discoveries vs favorites)
- Shows diversity of genre across top artists
- Provides time-based artist ranking changes
*/

{{
    config(
        materialized='table',
        schema='reporting',
        tags=['report', 'daily']
    )
}}
