-- stg_spotify_user_listening_history.sql
/*
Purpose: Transform user listening history into an analytical format
This model:
- Structures streaming history with played_at timestamps
- Links each play event to track and artist IDs
- Calculates listen duration and completion rates
- Identifies skips vs. complete plays
- Adds context about device type and listening session
*/

{{
    config(
        materialized='view',
        schema='staging',
        tags=['daily_refresh']
    )
}}