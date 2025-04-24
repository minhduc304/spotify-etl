-- playlist_composition_metrics.sql
/*
Purpose: Analyze the structure and composition of playlists
This model:
- Measures playlist diversity across various dimensions
- Calculates coherence scores for playlist tracks
- Tracks how playlists evolve over time
- Computes recommendation potential based on user history
*/

{{
    config(
        materialized='table',
        schema='analytics',
        tags=['daily_refresh', 'playlist_insights']
    )
}}

with playlists as (
    select * from {{ ref('stg_spotify_playlists') }}
),

playlist_tracks as (
    select 
        playlist_id,
        track_id,
        position,
        added_at,
        added_by
    from {{ source('spotify', 'playlist_tracks') }}
),

tracks as (
    select * from {{ ref('stg_spotify_tracks') }}
),

artists as (
    select * from {{ ref('stg_spotify_artists') }}
),

albums as (
    select * from {{ ref('stg_spotify_albums') }}
),

user_history as (
    select * from {{ ref('stg_spotify_user_listening_history') }}
),

-- Calculate temporal metrics (how playlists evolve over time)
playlist_temporal_metrics as (
    select
        pt.playlist_id,
        -- First track added date
        min(pt.added_at) as first_track_added_at,
        -- Last track added date
        max(pt.added_at) as last_track_added_at,
        -- Time span of playlist creation
        extract(day from max(pt.added_at) - min(pt.added_at)) as days_to_create,
        -- Recent modifications
        sum(case when pt.added_at >= current_date - interval '30 days' then 1 else 0 end) as tracks_added_last_30_days,
        -- Average tracks added per month (playlist growth rate)
        count(*) / greatest(1, extract(month from age(max(pt.added_at), min(pt.added_at)))) as avg_tracks_per_month
    from playlist_tracks pt
    group by pt.playlist_id
),

-- Get audio features for full coherence analysis
playlist_track_features as (
    select
        pt.playlist_id,
        pt.position,
        t.track_id,
        t.track_name,
        t.popularity,
        t.explicit,
        t.duration_ms,
        t.primary_artist_id,
        a.genres as artist_genres,
        a.popularity as artist_popularity,
        alb.release_date,
        -- Calculate track age in days
        extract(day from current_date - alb.release_date) as track_age_days,
        pt.added_at
    from playlist_tracks pt
    join tracks t on pt.track_id = t.track_id
    left join artists a on t.primary_artist_id = a.artist_id
    left join albums alb on t.album_id = alb.album_id
),

-- Calculate diversity metrics
playlist_diversity as (
    select
        ptf.playlist_id,
        -- Artist diversity
        count(distinct ptf.primary_artist_id) as unique_artists_count,
        count(distinct ptf.primary_artist_id)::float / count(*)::float as artist_diversity_ratio,
        -- Era diversity (decades)
        count(distinct floor(extract(year from alb.release_date) / 10)) as decade_count,
        array_agg(distinct floor(extract(year from alb.release_date) / 10)) as decades_present,
        -- Genre diversity
        count(distinct unnest(ptf.artist_genres)) as unique_genres_count,
        -- Popularity diversity - standard deviation of track popularity
        stddev(ptf.popularity) as popularity_std_dev,
        -- Age diversity - range between oldest and newest track
        max(alb.release_date) - min(alb.release_date) as release_date_range_days,
        -- Calculate % of tracks from each decade
        sum(case when extract(year from alb.release_date) >= 2020 then 1 else 0 end)::float / count(*)::float as pct_2020s,
        sum(case when extract(year from alb.release_date) between 2010 and 2019 then 1 else 0 end)::float / count(*)::float as pct_2010s,
        sum(case when extract(year from alb.release_date) between 2000 and 2009 then 1 else 0 end)::float / count(*)::float as pct_2000s,
        sum(case when extract(year from alb.release_date) between 1990 and 1999 then 1 else 0 end)::float / count(*)::float as pct_1990s,
        sum(case when extract(year from alb.release_date) between 1980 and 1989 then 1 else 0 end)::float / count(*)::float as pct_1980s,
        sum(case when extract(year from alb.release_date) < 1980 then 1 else 0 end)::float / count(*)::float as pct_pre_1980s
    from playlist_track_features ptf
    left join albums alb on ptf.track_id = tracks.album_id
    group by ptf.playlist_id
),

-- Calculate coherence scores
playlist_coherence as (
    select
        ptf.playlist_id,
        -- Artist coherence (lower score = more focus on specific artists)
        1 - (count(distinct ptf.primary_artist_id)::float / count(*)::float) as artist_coherence_score,
        -- Genre coherence
        1 - (count(distinct unnest(ptf.artist_genres))::float / greatest(1, sum(array_length(ptf.artist_genres, 1)))::float) as genre_coherence_score,
        -- Era coherence (standard deviation of release years - lower is more coherent)
        stddev(extract(year from alb.release_date)) as release_year_std_dev,
        -- Popularity coherence (lower std dev = more coherent popularity level)
        case
            when stddev(ptf.popularity) <= 10 then 'very_high'
            when stddev(ptf.popularity) <= 15 then 'high'
            when stddev(ptf.popularity) <= 20 then 'medium'
            when stddev(ptf.popularity) <= 25 then 'low'
            else 'very_low'
        end as popularity_coherence
    from playlist_track_features ptf
    left join albums alb on ptf.track_id = tracks.album_id
    group by ptf.playlist_id
),

-- Analyze flow of tracks in playlist (sequence analysis)
playlist_flow as (
    select
        ptf1.playlist_id,
        -- Average transition in popularity between consecutive tracks
        avg(abs(ptf2.popularity - ptf1.popularity)) as avg_popularity_transition,
        -- Consistent flow vs jarring transitions
        case
            when avg(abs(ptf2.popularity - ptf1.popularity)) <= 10 then 'smooth'
            when avg(abs(ptf2.popularity - ptf1.popularity)) <= 20 then 'moderate'
            else 'jarring'
        end as popularity_flow_type,
        -- Analyze tempo progression
        case 
            when corr(ptf1.position, ptf1.popularity) > 0.3 then 'building_up'
            when corr(ptf1.position, ptf1.popularity) < -0.3 then 'winding_down'
            else 'fluctuating'
        end as popularity_progression
    from playlist_track_features ptf1
    left join playlist_track_features ptf2 
        on ptf1.playlist_id = ptf2.playlist_id 
        and ptf1.position + 1 = ptf2.position
    where ptf2.playlist_id is not null
    group by ptf1.playlist_id
),

-- Calculate recommendation potential based on user history
recommendation_potential as (
    select
        p.playlist_id,
        p.playlist_name,
        -- Count of tracks user has listened to from this playlist
        count(distinct uh.track_id) as tracks_in_listening_history,
        -- Percent of playlist already heard
        count(distinct uh.track_id)::float / p.track_count::float as pct_playlist_heard,
        -- Average completion rate when user listened to these tracks
        avg(uh.completion_percentage) as avg_completion_rate,
        -- Skip rate for tracks from this playlist
        sum(case when uh.is_skipped then 1 else 0 end)::float / count(distinct uh.track_id)::float as skip_rate,
        -- Calculate recommendation score (higher = better recommendation)
        case
            -- If user has listened to >20% of playlist with good completion, high potential
            when count(distinct uh.track_id)::float / p.track_count::float > 0.2 
                and avg(uh.completion_percentage) > 80 then 'high'
            -- If user has listened to some tracks with decent completion, medium potential
            when count(distinct uh.track_id)::float / p.track_count::float > 0.05 
                and avg(uh.completion_percentage) > 60 then 'medium'
            -- If user has listened to very few tracks or skips often, low potential
            when count(distinct uh.track_id) > 0 then 'low'
            -- If user has never listened to any track, unknown potential
            else 'unknown'
        end as recommendation_potential
    from playlists p
    join playlist_tracks pt on p.playlist_id = pt.playlist_id
    left join user_history uh on pt.track_id = uh.track_id
    group by p.playlist_id, p.playlist_name, p.track_count
)

-- Final combined metrics
select
    p.playlist_id,
    p.playlist_name,
    p.description,
    p.owner_id,
    p.playlist_type,
    p.track_count,
    p.duration_minutes,
    p.duration_formatted,
    
    -- Basic diversity metrics from staging model
    p.unique_artists_count,
    p.unique_albums_count,
    p.artist_diversity_ratio,
    p.album_diversity_ratio,
    
    -- Enhanced diversity metrics
    pd.unique_genres_count,
    pd.decade_count,
    pd.decades_present,
    pd.popularity_std_dev,
    pd.release_date_range_days,
    
    -- Era distribution
    pd.pct_2020s,
    pd.pct_2010s,
    pd.pct_2000s,
    pd.pct_1990s,
    pd.pct_1980s,
    pd.pct_pre_1980s,
    
    -- Coherence scores
    pc.artist_coherence_score,
    pc.genre_coherence_score,
    pc.release_year_std_dev,
    pc.popularity_coherence,
    
    -- Create overall coherence rating
    case
        when pc.artist_coherence_score > 0.7 and pc.genre_coherence_score > 0.6 then 'very_high'
        when pc.artist_coherence_score > 0.5 and pc.genre_coherence_score > 0.4 then 'high'
        when pc.artist_coherence_score > 0.3 and pc.genre_coherence_score > 0.3 then 'medium'
        when pc.artist_coherence_score > 0.2 and pc.genre_coherence_score > 0.2 then 'low'
        else 'very_low'
    end as overall_coherence_rating,
    
    -- Flow metrics
    pf.avg_popularity_transition,
    pf.popularity_flow_type,
    pf.popularity_progression,
    
    -- Temporal metrics
    ptm.first_track_added_at,
    ptm.last_track_added_at,
    ptm.days_to_create,
    ptm.tracks_added_last_30_days,
    ptm.avg_tracks_per_month,
    
    -- Recommendation potential
    rp.tracks_in_listening_history,
    rp.pct_playlist_heard,
    rp.avg_completion_rate,
    rp.skip_rate,
    rp.recommendation_potential,
    
    -- Additional insights
    case
        when p.track_count >= 100 and pd.decade_count >= 4 and pd.unique_genres_count >= 10 then 'eclectic_mix'
        when pc.artist_coherence_score > 0.8 then 'artist_focused'
        when pc.genre_coherence_score > 0.8 then 'genre_focused'
        when pd.pct_pre_1980s > 0.7 then 'nostalgic'
        when pd.pct_2020s > 0.8 then 'current_hits'
        when pf.popularity_progression = 'building_up' then 'energy_builder'
        when pf.popularity_progression = 'winding_down' then 'wind_down'
        when p.playlist_name ilike '%workout%' or p.playlist_name ilike '%gym%' then 'workout_potential' 
        when p.playlist_name ilike '%sleep%' or p.playlist_name ilike '%relax%' then 'relaxation_potential'
        when p.playlist_name ilike '%party%' or p.playlist_name ilike '%dance%' then 'party_potential'
        else 'general_mix'
    end as playlist_character,
    
    -- New metric: playlist freshness score (combines content age and update recency)
    (
        case
            when p.freshness_category = 'very_fresh' then 1.0
            when p.freshness_category = 'fresh' then 0.8
            when p.freshness_category = 'recent' then 0.6
            when p.freshness_category = 'stale' then 0.3
            else 0.0
        end * 
        case
            when pd.pct_2020s > 0.8 then 1.0
            when pd.pct_2020s > 0.6 then 0.8
            when pd.pct_2020s > 0.4 then 0.6
            when pd.pct_2020s > 0.2 then 0.4
            else 0.2
        end
    ) * 100 as freshness_score,
    
    -- Data quality flags
    case 
        when p.track_count = 0 then true 
        else false 
    end as is_empty_playlist,
    
    current_timestamp as processed_at

from playlists p
left join playlist_diversity pd on p.playlist_id = pd.playlist_id
left join playlist_coherence pc on p.playlist_id = pc.playlist_id
left join playlist_flow pf on p.playlist_id = pf.playlist_id
left join playlist_temporal_metrics ptm on p.playlist_id = ptm.playlist_id
left join recommendation_potential rp on p.playlist_id = rp.playlist_id
