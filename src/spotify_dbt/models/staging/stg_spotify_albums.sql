-- stg_spotify_albums.sql
/*
Purpose: Standardize album information
This model:
- Structures album metadata (name, type, release date)
- Links albums to artists with proper artist roles
- Processes album images and external URLs
- Handles various album types (album, single, compilation)
- Preserves market availability information
*/

{{ 
    config(
        materialized='view',
        schema='staging',
        tags=['daily_refresh']
    ) 
}}

with albums_source as (
    select 
        album_id,
        name,
        album_type,
        release_date,
        release_date_precision,
        total_tracks,
        added_at
    from {{ source('spotify', 'albums') }}    
), 

album_artists_source (
    select 
        artist_id,
        name as artist_name,
    from {{ source('spotify', 'artists') }}    
), 

standardized_dates (
    select 
        album_id,
        name,
        album_type,
        case 
            when release_date_precision = 'day' then release_date::date
            when release_date_precision = 'month' then (release_date || '-01')::date
            when release_date_precision = 'year' then (release_date || '-01-01')::date
            else null
        end as release_date_standardized,
        release_date_precision,
        total_tracks,
        added_at
    from albums_source
),
 
albums_with_artists as (
    select 
        a.album_id,
        a.name as album_name,
        a.album_type,
        a.release_date,
        a.release_date_precision,
        a.total_tracks,
        a.added_at,
        array_agg(distinct art.artist_id) as artist_ids,
        array_agg(distinct art.artist_name) as artist_names
    from standardized_dates a
    left join album_artists_source aa on a.album_id = aa.album_id
    left join artists_source art aa.artist_id = art.artist_id
    group by 
        a.album_id,
        a.name,
        a.album_type,
        a.release_date,
        a.release_date_precision,
        a.total_tracks,
        a.added_at
)

select 
    album_id, 
    album_name,
    album_type,
    release_date_standardized as release_date,
    release_date_precision,
    total_tracks,
    artist_ids,
    artist_names,
    added_at,
    -- Calculate album age in days
    case
        when release_date_standardized is not null
        then extract(day from current_date - release_date_standardized)
        else null
    end as days_since_release,
    -- Categorize album freshness
    case
        when release_date_standardized >= current_date - interval '30 days' then 'new_release'
        when release_date_standardized >= current_date - interval '1 year' then 'recent'
        when release_date_standardized >= current_date - interval '5 year' then 'standard'
        else 'catalog'
    end as release_category,
    --Extract year for easy grouping
    extract(year from release_date_standardized) as release_year
from albums_with_artists




