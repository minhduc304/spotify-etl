-- user_listening_behavior.sql
/*
Purpose: Create rich user behavior metrics from listening history
This model:
- Calculates daily/hourly/weekly listening patterns
- Identifies peak listening times and days
- Measures listening session durations and frequency
- Computes track diversity and artist exploration metrics
- Analyzes listening continuity and engagement patterns
- Tracks changes in listening behavior over time
*/


{{
    config(
        materialized='table',
        schema='analytics',
        sort=date_day,
        dist='user_id',
        tags=['daily']

    )
}}

with listening_history as (
    select * from {{ ref('stg_spotify_user_listening_history') }}
),

daily_aggregates as (
    select 
        user_id,
        date_trunc('day', played_at) as date_day,
        count(*) as tracks_played,
        count(distinct track_id) as unique_tracks,
        count(distinct artist_id) as unique_artists,
        min(played_at) as first_played,
        max(played_at) as last_played,
        extract(epoch from (max(played_at) - min(played_at)))/3600 as session_duration_hours
    from listening_history
    group_by 1, 2
),

hourly_distribution as (
    select 
        user_id,
        extract(hour from played_at) as hour_of_day,
        count(*) as tracks_played,
    from listening_history
    group by 1, 2
),

final as (
    select 
        d.user_id,
        d.date_day,
        d.tracks_played,
        d.unique_tracks,
        d.unique_artists,
        d.session_duration_hours,
        d.tracks_played / nullif(d.session_duration_hours, 0) as tracks_per_hour,
        d.tracks_played / nullif(d.unique_tracks, 0) as tracks_diversity_ratio,
        d.tracks_played / nullif(d.unique_artists, 0) as artist_exploration_ratio,
        extract(dow from d.date_day) as day_of_week
    from daily_aggregates d
)

select * from final