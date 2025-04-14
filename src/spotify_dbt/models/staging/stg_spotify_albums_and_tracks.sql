-- stg_spotify_albums.sql
/*
Purpose: Standardize album information
This model:
- Structures album metadata (name, type, release date)
- Links albums to artists with proper artist roles
- Processes album images and external URLs
- Handles various album types (album, single, compilation)
- Preserves market availability information
*/

{{ 
    config(
        materialized='view',
        schema='staging',
        tags=['daily_refresh']
    ) 
}}


-- stg_spotify_tracks.sql
/*
Purpose: Clean and standardize raw track data from Spotify API
This model:
- Extracts track metadata (id, name, popularity, duration_ms, etc)
- Standardizes audio features (danceability, energy, valence, etc)
- Adds consistent formatting for track URIs and external URLs
- Handles missing values and data type conversions
- Adds metadata about when the data was loaded
*/

{{
    config(
        materialized='view',
        schema='staging',
        tags=['daily_refresh']
    )
}}