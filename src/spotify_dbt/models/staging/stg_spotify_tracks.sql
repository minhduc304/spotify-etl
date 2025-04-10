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