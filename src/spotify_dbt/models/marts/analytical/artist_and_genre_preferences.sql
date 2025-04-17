-- artist_and_genre_preferences.sql
/*
Purpose: Core analytical model for artist and genre affinity calculation
This model:
- Calculates standardized user-specific artist affinity scores
- Measures objective artist popularity metrics over time
- Creates user-to-genre affinity scores based on listening history
- Identifies primary, secondary, and tertiary genre preferences
- Analyzes raw genre diversity in listening habits
- Calculates base metrics for genre transitional patterns

- Sources from stg_spotify_artists and stg_spotify_user_listening_history
*/

{{
    config(
        materialized='table',
        schema='analytics',
        tags=['daily', 'artist_insights', 'genre_insights']
    )
}}

-- 1. Create artist interaction events with appropriate weights
-- 2. Apply affinity scores macro for artists
-- 3. Add metadata for artist affinities
-- 4. Create genre interactions by unnesting artist genres
-- 5. Apply affinity scores macro for genres
-- 6. Analyze genre preferences in more detail 
-- 7. Calculate genre transition frequencies
-- 8. Combine final outputs
-- Artist preferences ranking 
-- Genre preferences ranking 
-- Genre transitions 

