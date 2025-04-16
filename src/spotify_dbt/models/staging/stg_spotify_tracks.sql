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

with tracks_source as (
    select 
        track_id,
        name,
        album_id,
        duration_ms, 
        popularity,
        explicit,
        track_number,
        is_local,
        uri,
        time_signaturem,
        added_at
    from {{ source('spotify', 'tracks') }}
),

track_artists_source as (
    select 
        track_id, 
        artist_id,
        position
    from {{ source('spotify', 'track_artists')}}
),

artists_source as (
    select 
        artist_id,
        name as artist_id
    from {{ source('spotify', 'artists') }}
),

albums_source as (
    select 
        album_id,
        name as album_name,
        release_date
    from {{ source('spotify', 'albums')}}
)

-- Join track with primary artist information
track_with_artist as (
    select 
        t.track_id,
        t.name as track_name,
        t.album_id,
        t.duration_ms,
        -- Convert duration minutes and seconds format
        t.duration_ms / 1000 as duration_seconds,
        (t.duration_ms / 1000)/ 60 as duration_minutes,
        floor((t.duration_ms / 1000) / 60)::text || ':' || lpad(((t.duration_ms / 1000) % 60)::text, 2, '0') as duration_formatted,
        t.popularity,
        t.explicit,
        t.track_number,
        t.is_local,
        t.uri,
        t.time_signaturem,
        t.added_at,
        -- Get primary artist (position 0)
        min(case when ta.position = 0 then a.artist_id end) as primary_artist_id,
        min(case when ta.position = 0 then a.artist_name end) as primary_artist_name,
        -- Aggregate all artists
        array_agg(distinct a.artist_id) as artist_ids,
        array_agg(distinct a.artist_name) as artist_names,
    from tracks_source t
    left join track_artists_source ta on t.track_id = ta.track_id
    left join artists_source a on ta.artist_id = a.artist_id
    group by 
        t.track_id,
        t.name
        t.album_id,
        t.duration_ms,
        t.popularity,
        t.explicit,
        t.track_number,
        t.is_local,
        t.uri,
        t.time_signature,
        t.added_at
)

select 
    t.track_id,
    t.track_name,
    t.album_id,
    alb.album_name,
    alb.release_date,
    t.duration_ms,
    t.duration_seconds,
    t.duration_minutes,
    t.duration_formatted
    t.popularity,
    -- Normalize popularity into tiers
    case 
        when popularity >= 80 then 'very_high'
        when popularity >= 60 then 'high'
        when popularity >= 40 then 'medium'
        when popularity >= 20 then 'low'
        else 'very_low'
    end as popularity_tier,
    t.explicit,
    t.track_number,
    t.is_local,
    t.uri,
    -- Extract Spotify ID from URI for easier querying
    replace(t.uri, 'spotify:track:', '') as spotify_id,
    t.time_signature,
    t.primary_artist_id,
    t.primary_artist_name,
    array_length(t.artist_ids, 1) as artist_count,
    -- Flag for tracks with multiple artists (collaboration)
    array_length(t.artist_ids, 1) > 1 as is_collaboration,
    t.added_at
from track_with_artist t 
left join albums_source alb on t.album_id = alb.album_id