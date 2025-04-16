-- playlist_diversity_report.sql
/*
Purpose: Report on compositional diversity of playlists
This model:
- Creates genre distribution metrics for playlists
- Calculates freshness metrics (new releases vs classics)
- Analyzes artist diversity within playlists
- Compares user playlists to Spotify's editorial playlists
- Source from stg_spotify_artists, stg_spotify_albums_and_tracks, stg_spotify_playlists
*/


{{
    config(
        materialized='table',
        schema='reporting',
        tags=['report', 'weekly']
    )
}}

