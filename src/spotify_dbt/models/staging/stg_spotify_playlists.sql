-- stg_spotify_playlists.sql
/*
Purpose: Structure playlist data for analysis
This model:
- Normalizes playlist metadata (name, description, etc)
- Links playlist-track relationships with position information
- Processes playlist follower counts and public/private status
- Handles collaborative playlist indicators
- Preserves playlist owner information
*/

{{ 
    config(
        materialized='view',
        schema='staging',
        tags=['daily_refresh']
    ) 
}}