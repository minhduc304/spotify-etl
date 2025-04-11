-- top_artists_report.sql
/*
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

