-- stg_spotify_artists.sql
/*
Purpose: Normalize artist data from Spotify API
This model:
- Extracts core artist information (id, name, popularity, followers)
- Processes genre lists into a standardized format
- Handles artist images and external URLs
- Creates a consistent view of artist metadata
- Incorporates artist type and market availability
*/

{{ 
    config(
        materialized='view',
        schema='staging',
        tags=['daily_refresh']
    ) 
}}