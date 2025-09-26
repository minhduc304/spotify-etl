-- stg_spotify_user_listening_history.sql
/*
Purpose: Transform user listening history into an analytical format
This model:
- Structures streaming history with played_at timestamps
- Links each play event to track and artist IDs
- Calculates listen duration and completion rates
- Identifies skips vs. complete plays
- Adds context about device type and listening session
*/

{{
    config(
        materialized='view',
        schema='staging',
        tags=['daily_refresh']
    )
}}

with listening_history_source as (
    select 
        id, 
        track_id,
        played_at,
        ms_played,
        context_type,
        context_id
    from {{ source('spotify', 'user_listening_history') }}
),

tracks_source as (
    select 
        track_id,
        track_name,
        album_id,
        duration_ms,
        popularity,
        explicit,
        primary_artist_id
    from {{ ref('stg_spotify_tracks') }}
),

albums_source as (
    select 
        album_id,
        album_name, 
        release_date
    from {{ ref('stg_spotify_albums') }}
),

-- Add time gaps for session identification
listening_with_gaps as (
    select 
        id, 
        track_id,
        played_at,
        ms_played,
        context_type,
        context_id,
        lag(played_at) over (order by played_at) as prev_played_at
    from listening_history_source
),

-- Add session identification
-- Sessions are defined as continuous listening with gaps less than 30 minutes long
listening_with_sessions as (
    select 
        id, 
        track_id,
        played_at,
        ms_played,
        context_type,
        context_id,
        -- Identify sessions based on time gaps 
        sum(
            case
                when played_at - prev_played_at > interval '30 minutes'
                or prev_played_at is null
                then 1
                else 0
            end
        ) over (order by played_at rows unbounded preceding) as session_id
    from listening_with_gaps
),
-- Process context_types
listening_contexts as (
    select 
        context_type,
        context_id,
        -- Extract context entity from URI
        case 
            when context_type = 'playlist' then replace(context_id, 'spotify:playlist:', '')
            when context_type = 'album' then replace(context_id, 'spotify:album:', '')
            when context_type = 'artist' then replace(context_id, 'spotify:artist:', '')
            else context_id
        end as context_entity_id
    from listening_history_source
    where context_id is not null
    group by context_type, context_id
)

select 
    lh.id,
    lh.track_id,
    t.track_name,
    t.album_id,
    a.album_name,
    t.primary_artist_id,
    lh.played_at,
    -- Extract time parts for time-of-day analysis
    extract(hour from lh.played_at) as hour_of_day,
    extract(dow from lh.played_at) as day_of_week,
    extract(week from lh.played_at) as week_of_year,
    extract(month from lh.played_at) as month,
    extract(year from lh.played_at) as year,
    -- Time-of-day categories
    case
        when extract(hour from lh.played_at) between 5 and 11 then 'morning'
        when extract(hour from lh.played_at) between 12 and 17 then 'afternoon'
        when extract(hour from lh.played_at) between 18 and 21 then 'evening'
        else 'night'
    end as time_of_day,
    -- Day type
    case 
        when extract(dow from lh.played_at) in (0, 6) then 'weekend'
        else 'weekday'
    end as day_type,
    lh.ms_played,
    t.duration_ms,
    -- Calculate completion rate
    round((lh.ms_played::numeric / t.duration_ms::numeric) * 100, 2) as completion_percentage,
    -- Flag for skipped tracks
    case 
        when lh.ms_played < t.duration_ms * 0.3 then true
        else false
    end as is_skipped,
    -- Flag for completed tracks
    case 
        when lh.ms_played > t.duration_ms * 0.9 then true
        else false
    end as is_completed,
    lh.context_type,
    lh.context_id,
    lh.session_id,
    -- Calculate position within the session
    row_number() over (partition by lh.session_id order by lh.played_at) as session_position
from listening_with_sessions lh
join tracks_source t on lh.track_id = t.track_id
left join albums_source a on t.album_id = a.album_id