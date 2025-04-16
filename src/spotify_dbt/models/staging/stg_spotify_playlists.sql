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

with playlists_source as (
    select 
        playlist_id, 
        name as playlist_name,
        description,
        owner_id,
        is_public,
        is_collaborative,
        snapshot_id,
        added_at
    from {{ source('spotify', 'playlists') }}
)

playlist_tracks_source as (
    select 
        playlist_id, 
        track_id,
        position,
        added_at,
        added_by
    from {{ source('spotify', 'playlist_tracks') }}
)

tracks_source as (
    select 
        track_id,
        track_name, 
        album_id,
        primary_artist_id,
        primary_artist_name,
        duration_ms,
        popularity,
        explicit
    from {{ ref('stg_spotify_tracks') }}
),

-- Calculate metrics for each playlist
playlist_metrics as (
    select 
        pt.playlist_id,
        count(distinct pt.track_id) as track_count,
        sum(pt.duration_ms) as total_duration_ms,
        -- Average track metrics
        avg(t.popularity) as avg_track_popularity,
        count(distinct t.primary_artist_id) as unique_artists_count,
        count(distinct t.album_id) as unique_albums_count,
        -- Explicit content metrics 
        sum(case when t.explicit then 1 else 0) as explicit_tracks_count,
        -- When was playlist last updated
        max(pt.added_at) as last_updated_at
    from playlist_tracks_source pt
    left join tracks_source t on pt.track_id = t.track_id
    group by pt.playlist_id
)

select 
    p.playlist_id,
    p.name as playlist_name,
    p.description,
    p.owner_id,
    case 
        when p.owner_id = 'spotify_official' then 'Spotify'
        else user_created
    end as playlist_type
    p.is_public,
    p.is_collaborative,
    p.snapshot_id,
    p.added_at,
    -- Join with calculated metrics
    coalesce(pm.track_count, 0) as track_count,
    -- Format durations
    coalesce(pm.duration_ms, 0) as total_duration_ms,
    -- Convert to minutes
    round((coalesce(pm.duration_ms, 0)/ 1000)/ 60, 2) as duration_minutes,
    -- Convert to hours:minutes
    floor((coalesce(pm.duration_ms, 0)/ 1000/ 3600))::text || ':' || 
        lpad((((coalesce(pm.duration_ms, 0)/ 1000) % 3600) / 60)::text, 2, '0') as duration_formatted,
    coalesce(pm.avg_track_popularity, 0) as avg_track_popularity,
    coalesce(pm.unique_artists_count, 0) as unique_artists_count,
    coalesce(pm.unique_albums_count, 0) as unique_albums_count,
    coalesce(pm.explicit_tracks_count, 0) as explicit_tracks_count,
    -- Calculate playlist diversity
    case 
        when coalesce(pm.track_count, 0) > 0
        then round(coalesce(pm.unique_artists_count, 0)::decimal / coalesce(pm.track_count, 1)::decimal, 2)
        else 0
    end as artist_diversity_ratio,
    case 
        when coalesce(pm.track_count, 0) > 0
        then round(coalesce(pm.unique_albums_count, 0)::decimal / coalesce(pm.track_count, 1)::decimal, 2)
        else 0
    end as album_diversity_ratio,
    -- Content flags
    case
        when coalesce(pm.explicit_tracks_count, 0) > 0 then true
        else false
    end as contains_explicit_content,
    -- Size categories
    case
        when coalesce(pm.track_count, 0) >= 100 then 'large'
        when coalesce(pm.track_count, 0) >= 50 then 'medium'
        when coalesce(pm.track_count, 0) >= 10 then 'small'
        else 'mini'
    end as playlist_size_category,
    pm.last_updated_at,
    -- Freshness: days since last update
    case
        when pm.last_updated_at is not null 
        then extract(day from current_timestamp - pm.last_updated_at)
        else null
    end as days_since_update,
    -- Freshness category
    case
        when pm.last_updated_at is null then 'unknown'
        when pm.last_updated_at >= current_timestamp - interval '7 days' then 'very_fresh'
        when pm.last_updated_at >= current_timestamp - interval '30 days' then 'fresh'
        when pm.last_updated_at >= current_timestamp - interval '90 days' then 'recent'
        else 'stale'
    end as freshness_category
from playlists_source p
left join playlist_metrics pm on p.playlist_id = pm.playlist_id
    