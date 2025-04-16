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

with artists_source as (
    select 
        album_id,
        artist_id,
        popularity,
        followers,
        genres::json as genres_json,
        added_at
    from {{ source('spotify', 'artists') }}
),

-- Track relationships for calculating artist metric
track_artists_source as (
    select 
        artist_id,
        count(distinct track_id) as track_count
    from {{ source('spotify', 'track_artists') }}
    group by artist_id
),

-- Album relationships
album_artists_source as (
    select 
        artist_id,
        count(distinct album_id) as album_count
    from {{ source('spotify', 'album_artists') }}
    group by artist_id
),

-- Process genres to more queryable format
genre_processing as (
    select 
        artist_id,
        name,
        popularity,
        followers,
        genres_json,
        jsonb_array_elements_text(genres_json::jsonb) as genre,
        added_at
    from artists_source
),

-- Aggregate genres back
artist_genres as (
    select 
        artist_id,
        array_agg(distinct genres) as genres,
        array_length(array_agg(distinct genres)) as genre_count
    from genre_processing
    group by artist_id
)

select 
    a.artist_id,
    a.name,
    a.popularity,
    -- Normalize popularity into tier categories
    case 
        when a.popularity >= 80 then 'very_high'
        when a.popularity >= 60 then 'high'
        when a.popularity >= 40 then 'medium'
        when a.popularity >= 20 then 'low'
        else 'very_low'
    end as popularity_tier,
    a.followers,
    -- Follower tiers
    case 
        when a.followers >= 5000000 then 'mega'
        when a.followers >= 1000000 then 'major'
        when a.followers >= 500000 then 'large'
        when a.followers >= 100000 then 'medium'
        when a.followers >= 10000 then 'small'
        else 'micro'
    end as followers_tier,
    -- Join with processed genres
    ag.genres,
    ag.genre_count,
    -- Primary genre (first in the list)
    case 
        when array_length(ag.genres, 1) > 0
        then ag.genres[1]
        else 'unknown'
    end as primary_genre,
    -- Content metrics
    coalesce(ta.track_count, 0) as track_count,
    coalesce(aa.album_count, 0) as album_count,
    a.added_at
from artists_source a
left join artist_genres ag on a.artist_id = ag.artist_id
left join track_artists_source ta on a.artist_id = ta.artist_id
left join album_artists_source aa on a.artist_id = aa.artist_id


