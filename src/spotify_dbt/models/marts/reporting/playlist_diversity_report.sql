-- playlist_diversity_report.sql
/*
Purpose: Report on compositional diversity of playlists
This model:
- Creates genre distribution metrics for playlists
- Calculates freshness metrics (new releases vs classics)
- Analyzes artist diversity within playlists
- Compares user playlists to Spotify's editorial playlists
- Source from stg_spotify_artists, stg_spotify_albums, stg_spotify_tracks, stg_spotify_playlists
*/

{{
    config(
        materialized='table',
        schema='reporting',
        tags=['report', 'weekly']
    )
}}

with playlist_tracks as (
    -- Get all tracks in playlists with their position
    select
        p.playlist_id,
        p.playlist_name,
        p.playlist_type,
        p.owner_id,
        pt.track_id,
        pt.position as track_position
    from {{ ref('stg_spotify_playlists') }} p
    join {{ source('spotify', 'playlist_tracks') }} pt on p.playlist_id = pt.playlist_id
),

track_details as (
    -- Join track information with album and artist details
    select
        pt.playlist_id,
        pt.playlist_name,
        pt.playlist_type,
        pt.owner_id,
        pt.track_id,
        pt.track_position,
        t.track_name,
        t.album_id,
        t.duration_seconds,
        t.popularity as track_popularity,
        t.popularity_tier as track_popularity_tier,
        t.explicit,
        t.primary_artist_id,
        t.primary_artist_name,
        t.artist_count,
        t.is_collaboration,
        a.album_name,
        a.release_date,
        a.release_year,
        a.release_category,
        a.days_since_release
    from playlist_tracks pt
    join {{ ref('stg_spotify_tracks') }} t on pt.track_id = t.track_id
    join {{ ref('stg_spotify_albums') }} a on t.album_id = a.album_id
),

artist_genres as (
    -- Get genre information for artists
    select
        td.playlist_id,
        td.track_id,
        td.primary_artist_id,
        ar.primary_genre,
        ar.genres,
        ar.genre_count,
        ar.popularity as artist_popularity,
        ar.popularity_tier as artist_popularity_tier,
        ar.followers,
        ar.followers_tier
    from track_details td
    left join {{ ref('stg_spotify_artists') }} ar on td.primary_artist_id = ar.artist_id
),

-- Calculate metrics per playlist
playlist_metrics as (
    select
        td.playlist_id,
        td.playlist_name,
        td.playlist_type,
        td.owner_id,
        
        -- Track count and duration metrics
        count(distinct td.track_id) as track_count,
        sum(td.duration_seconds) as total_duration_seconds,
        sum(td.duration_seconds)/60 as total_duration_minutes,
        
        -- Artist diversity metrics
        count(distinct td.primary_artist_id) as unique_artist_count,
        count(distinct td.primary_artist_id)::float / nullif(count(distinct td.track_id), 0) as artist_track_ratio,
        
        -- Release freshness metrics
        avg(td.days_since_release) as avg_track_age_days,
        count(case when td.release_category = 'new_release' then 1 end)::float / 
            nullif(count(distinct td.track_id), 0) as new_release_ratio,
        count(case when td.release_category = 'recent' then 1 end)::float / 
            nullif(count(distinct td.track_id), 0) as recent_release_ratio,
        count(case when td.release_category = 'standard' then 1 end)::float / 
            nullif(count(distinct td.track_id), 0) as standard_release_ratio,
        count(case when td.release_category = 'catalog' then 1 end)::float / 
            nullif(count(distinct td.track_id), 0) as catalog_release_ratio,
        
        -- Collaboration metrics
        count(case when td.is_collaboration = true then 1 end)::float / 
            nullif(count(distinct td.track_id), 0) as collaboration_ratio,
            
        -- Explicit content ratio
        count(case when td.explicit = true then 1 end)::float / 
            nullif(count(distinct td.track_id), 0) as explicit_content_ratio,
            
        -- Popularity metrics
        avg(td.track_popularity) as avg_track_popularity,
        count(case when td.track_popularity_tier = 'very_high' then 1 end)::float / 
            nullif(count(distinct td.track_id), 0) as very_high_popularity_ratio,
        count(case when td.track_popularity_tier = 'high' then 1 end)::float / 
            nullif(count(distinct td.track_id), 0) as high_popularity_ratio,
        count(case when td.track_popularity_tier = 'medium' then 1 end)::float / 
            nullif(count(distinct td.track_id), 0) as medium_popularity_ratio,
        count(case when td.track_popularity_tier = 'low' then 1 end)::float / 
            nullif(count(distinct td.track_id), 0) as low_popularity_ratio,
        count(case when td.track_popularity_tier = 'very_low' then 1 end)::float / 
            nullif(count(distinct td.track_id), 0) as very_low_popularity_ratio
    from track_details td
    group by td.playlist_id, td.playlist_name, td.playlist_type, td.owner_id
),

-- Genre diversity per playlist
genre_diversity as (
    select
        ag.playlist_id,
        count(distinct ag.primary_genre) as unique_genre_count,
        -- Primary genre distribution
        array_agg(distinct ag.primary_genre) as genres_in_playlist,
        
        -- Calculate genre entropy (measure of genre diversity)
        -- First get counts per genre 
        array_agg(ag.primary_genre) as all_genres_array
    from artist_genres ag
    group by ag.playlist_id
),

-- Calculate top 5 genres per playlist
playlist_top_genres as (
    select
        ag.playlist_id,
        primary_genre,
        count(*) as genre_count,
        count(*)::float / sum(count(*)) over (partition by ag.playlist_id) as genre_percentage,
        row_number() over (partition by ag.playlist_id order by count(*) desc) as genre_rank
    from artist_genres ag
    group by ag.playlist_id, primary_genre
),

top_genres_agg as (
    select
        playlist_id,
        array_agg(primary_genre order by genre_rank) filter (where genre_rank <= 5) as top_5_genres,
        array_agg(genre_percentage order by genre_rank) filter (where genre_rank <= 5) as top_5_genre_percentages
    from playlist_top_genres
    group by playlist_id
),

-- Calculate Herfindahl index for genre concentration (lower = more diverse)
genre_concentration as (
    select
        playlist_id,
        sum(power(genre_percentage, 2)) as herfindahl_index,
        case 
            when sum(power(genre_percentage, 2)) < 0.15 then 'Very Diverse'
            when sum(power(genre_percentage, 2)) < 0.25 then 'Diverse'
            when sum(power(genre_percentage, 2)) < 0.50 then 'Moderately Diverse' 
            when sum(power(genre_percentage, 2)) < 0.75 then 'Concentrated'
            else 'Highly Concentrated'
        end as genre_diversity_category
    from playlist_top_genres
    group by playlist_id
),

-- Artist popularity distribution
artist_popularity_dist as (
    select
        ag.playlist_id,
        count(case when ag.artist_popularity_tier = 'very_high' then 1 end)::float / 
            nullif(count(distinct ag.primary_artist_id), 0) as very_high_artist_ratio,
        count(case when ag.artist_popularity_tier = 'high' then 1 end)::float / 
            nullif(count(distinct ag.primary_artist_id), 0) as high_artist_ratio,
        count(case when ag.artist_popularity_tier = 'medium' then 1 end)::float / 
            nullif(count(distinct ag.primary_artist_id), 0) as medium_artist_ratio,
        count(case when ag.artist_popularity_tier = 'low' then 1 end)::float / 
            nullif(count(distinct ag.primary_artist_id), 0) as low_artist_ratio,
        count(case when ag.artist_popularity_tier = 'very_low' then 1 end)::float / 
            nullif(count(distinct ag.primary_artist_id), 0) as very_low_artist_ratio,
        
        -- Artist followers distribution
        count(case when ag.followers_tier = 'mega' then 1 end)::float / 
            nullif(count(distinct ag.primary_artist_id), 0) as mega_artist_ratio,
        count(case when ag.followers_tier = 'major' then 1 end)::float / 
            nullif(count(distinct ag.primary_artist_id), 0) as major_artist_ratio,
        count(case when ag.followers_tier = 'large' then 1 end)::float / 
            nullif(count(distinct ag.primary_artist_id), 0) as large_artist_ratio,
        count(case when ag.followers_tier = 'medium' then 1 end)::float / 
            nullif(count(distinct ag.primary_artist_id), 0) as medium_followers_artist_ratio,
        count(case when ag.followers_tier = 'small' then 1 end)::float / 
            nullif(count(distinct ag.primary_artist_id), 0) as small_artist_ratio,
        count(case when ag.followers_tier = 'micro' then 1 end)::float / 
            nullif(count(distinct ag.primary_artist_id), 0) as micro_artist_ratio
    from artist_genres ag
    group by ag.playlist_id
),

-- Decade distribution (for temporal diversity)
decade_distribution as (
    select
        td.playlist_id,
        floor(td.release_year / 10) * 10 as decade,
        count(*) as track_count,
        count(*)::float / sum(count(*)) over (partition by td.playlist_id) as decade_percentage
    from track_details td
    where td.release_year is not null
    group by td.playlist_id, floor(td.release_year / 10) * 10
),

decade_agg as (
    select
        playlist_id,
        count(distinct decade) as unique_decades,
        array_agg(decade || 's: ' || round(decade_percentage * 100) || '%' order by decade) as decade_breakdown
    from decade_distribution
    group by playlist_id
)

-- Final combined report
select
    pm.playlist_id,
    pm.playlist_name,
    pm.playlist_type,
    pm.owner_id,
    
    -- Basic metrics
    pm.track_count,
    pm.total_duration_minutes,
    
    -- Artist diversity
    pm.unique_artist_count,
    pm.artist_track_ratio,
    case
        when pm.artist_track_ratio >= 0.9 then 'Very High (>90% unique artists)'
        when pm.artist_track_ratio >= 0.7 then 'High (70-90% unique artists)'
        when pm.artist_track_ratio >= 0.5 then 'Medium (50-70% unique artists)'
        when pm.artist_track_ratio >= 0.3 then 'Low (30-50% unique artists)'
        else 'Very Low (<30% unique artists)'
    end as artist_diversity_category,
    
    -- Genre diversity
    gd.unique_genre_count,
    tg.top_5_genres,
    tg.top_5_genre_percentages,
    gc.herfindahl_index as genre_concentration_index,
    gc.genre_diversity_category,
    
    -- Release freshness
    pm.avg_track_age_days,
    pm.new_release_ratio,
    pm.recent_release_ratio,
    pm.standard_release_ratio,
    pm.catalog_release_ratio,
    da.unique_decades,
    da.decade_breakdown,
    case
        when pm.new_release_ratio >= 0.7 then 'Very Fresh (>70% new releases)'
        when pm.new_release_ratio + pm.recent_release_ratio >= 0.7 then 'Fresh (>70% within past year)'
        when pm.new_release_ratio + pm.recent_release_ratio + pm.standard_release_ratio >= 0.7 then 'Mixed (>70% within past 5 years)'
        else 'Catalog-focused (>30% older than 5 years)'
    end as freshness_category,
    
    -- Artist popularity metrics
    apd.very_high_artist_ratio,
    apd.high_artist_ratio,
    apd.medium_artist_ratio,
    apd.low_artist_ratio,
    apd.very_low_artist_ratio,
    case
        when apd.very_high_artist_ratio + apd.high_artist_ratio >= 0.8 then 'Mainstream'
        when apd.very_high_artist_ratio + apd.high_artist_ratio >= 0.5 then 'Popular-leaning'
        when apd.medium_artist_ratio >= 0.5 then 'Mid-tier Focus'
        when apd.low_artist_ratio + apd.very_low_artist_ratio >= 0.5 then 'Underground-leaning'
        when apd.low_artist_ratio + apd.very_low_artist_ratio >= 0.8 then 'Deep Underground'
        else 'Balanced'
    end as popularity_category,
    
    -- Artist followers metrics
    apd.mega_artist_ratio,
    apd.major_artist_ratio,
    apd.large_artist_ratio,
    apd.medium_followers_artist_ratio,
    apd.small_artist_ratio,
    apd.micro_artist_ratio,
    
    -- Additional metrics
    pm.collaboration_ratio,
    pm.explicit_content_ratio,
    pm.avg_track_popularity,
    
    -- Overall diversity score (composite index)
    (
        coalesce(pm.artist_track_ratio, 0) * 0.25 +
        coalesce(1 - gc.herfindahl_index, 0) * 0.4 +
        coalesce(da.unique_decades::float / 10, 0) * 0.2 +
        case
            when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.5 then 0.15
            when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.7 then 0.1
            when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.9 then 0.05
            else 0
        end
    ) as overall_diversity_score,
    
    -- Diversity category
    case
        when (
            coalesce(pm.artist_track_ratio, 0) * 0.25 +
            coalesce(1 - gc.herfindahl_index, 0) * 0.4 +
            coalesce(da.unique_decades::float / 10, 0) * 0.2 +
            case
                when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.5 then 0.15
                when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.7 then 0.1
                when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.9 then 0.05
                else 0
            end
        ) >= 0.7 then 'Very Diverse'
        when (
            coalesce(pm.artist_track_ratio, 0) * 0.25 +
            coalesce(1 - gc.herfindahl_index, 0) * 0.4 +
            coalesce(da.unique_decades::float / 10, 0) * 0.2 +
            case
                when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.5 then 0.15
                when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.7 then 0.1
                when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.9 then 0.05
                else 0
            end
        ) >= 0.5 then 'Diverse'
        when (
            coalesce(pm.artist_track_ratio, 0) * 0.25 +
            coalesce(1 - gc.herfindahl_index, 0) * 0.4 +
            coalesce(da.unique_decades::float / 10, 0) * 0.2 +
            case
                when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.5 then 0.15
                when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.7 then 0.1
                when apd.very_high_artist_ratio + apd.high_artist_ratio <= 0.9 then 0.05
                else 0
            end
        ) >= 0.3 then 'Moderately Diverse'
        else 'Narrowly Focused'
    end as overall_diversity_category
    
from playlist_metrics pm
left join genre_diversity gd on pm.playlist_id = gd.playlist_id
left join top_genres_agg tg on pm.playlist_id = tg.playlist_id
left join genre_concentration gc on pm.playlist_id = gc.playlist_id
left join artist_popularity_dist apd on pm.playlist_id = apd.playlist_id
left join decade_agg da on pm.playlist_id = da.playlist_id