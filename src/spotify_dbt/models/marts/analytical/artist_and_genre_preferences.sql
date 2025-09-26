-- artist_and_genre_preferences.sql
/*
Purpose: Core analytical model for artist and genre affinity calculation
This model:
- Calculates standardized user-specific artist affinity scores
- Measures objective artist popularity metrics over time
- Creates user-to-genre affinity scores based on listening history
- Identifies primary, secondary, and tertiary genre preferences
- Analyzes raw genre diversity in listening habits
- Calculates base metrics for genre transitional patterns

- Sources from stg_spotify_artists and stg_spotify_user_listening_history
*/

{{
    config(
        materialized='table',
        schema='analytics',
        tags=['daily', 'artist_insights', 'genre_insights']
    )
}}

-- 1. Create artist interaction events with appropriate weights
with artist_interactions as (
    select 
        h.primary_artist_id as artist_id,
        h.played_at,
        case 
            when h.is_completed then 'complete_listen'
            when h.is_skipped then 'skip'
            else 'partial_listen'
        end as event_type,
        h.completion_percentage,
        h.session_id,
        h.time_of_day,
        h.day_type
    from {{ ref('stg_spotify_user_listening_history') }} h
    where h.primary_artist_id is not null
),

-- 2. Calculate simple affinity scores for artists
artist_affinity as (
    select
        artist_id,
        count(case when event_type = 'complete_listen' then 1 end) * 1.0 +
        count(case when event_type = 'partial_listen' then 1 end) * 0.5 +
        count(case when event_type = 'skip' then 1 end) * (-0.25) as raw_score,
        -- Simple normalization to 0-10 scale
        least(10, greatest(0, 
            count(case when event_type = 'complete_listen' then 1 end) * 1.0 +
            count(case when event_type = 'partial_listen' then 1 end) * 0.5 +
            count(case when event_type = 'skip' then 1 end) * (-0.25)
        )) as normalized_score,
        count(*) as interaction_count,
        max(played_at) as last_interaction
    from artist_interactions
    group by artist_id
),

-- 3. Add metadata for artist affinities
artist_preferences as (
    select 
        a.artist_id,
        art.name as artist_name,
        a.normalized_score as artist_affinity_score,
        a.raw_score, 
        a.interaction_count,
        a.last_interaction,
        art.popularity as spotify_popularity,
        art.popularity_tier,
        art.followers,
        art.followers_tier,
        art.primary_genre,
        art.genres,
        art.genre_count,
        -- Categorize affinity
        case 
            when a.normalized_score >= 8.0 then 'favorite'
            when a.normalized_score >= 6.0 then 'highly_preferred'
            when a.normalized_score >= 4.0 then 'preferred'
            when a.normalized_score >= 2.0 then 'occasional'
            else 'rare'
        end as affinity_category
    from artist_affinity a
    join {{ ref('stg_spotify_artists') }} art on a.artist_id = art.artist_id
),

-- 4. Create genre interactions by unnesting artist genres
genre_interactions as (
    select 
        unnest(art.genres) as genre_name,
        h.played_at,
        case 
            when h.is_completed then 'complete_listen'
            when h.is_skipped then 'skip'
            else 'partial_listen'
        end as event_type
    from {{ ref('stg_spotify_user_listening_history') }} h 
    join {{ ref('stg_spotify_artists') }} art on h.primary_artist_id = art.artist_id
    where art.genres is not null and array_length(art.genres, 1) > 0
),

-- Step 5: Apply the affinity scores macro for genres
genre_affinity AS (
    {{ calculate_affinity_scores(
        table_name='genre_interactions',
        entity_id_column='genre_name',
        timestamp_column='played_at',
        event_type_column='event_type',
        event_weights={
            'complete_listen': 1.0, 
            'partial_listen': 0.5, 
            'skip': -0.25
        },
        time_decay_days=90,
        normalization_method='minmax',
        min_score=0,
        max_score=10
    ) }}
),

-- 6. Analyze genre preferences in more detail 
genre_preferences as (
    select 
        g.genre_name,
        g.normalized_score as genre_affinity_score,
        g.raw_score,
        g.interaction_count,
        g.last_interaction,
        -- Count distinct artists in this genre
        (select count(distinct art.artist_id)
        from {{ ref('stg_spotify_artists') }} art
        where   g.genre_name = any(art.genres)) as artist_count,
        -- Categorize affinity
        case 
            when g.normalized_score >= '8.0' then 'favourite'
            when g.normalized_score >= '6.0' then 'highly_preferrend'
            when g.normalized_score >= '4.0' then 'preferred'
            when g.normalized_score >= '2.0' then 'occasional'
            else 'rare'
        end as affinity_category
    from genre_affinity g
),

-- Genre transitions removed as primary_artist_genre column not available in staging

-- 8. Combine final outputs
final_outputs as (
    -- Artist preferences ranking 
    select 
        'artist' as preference_type,
        artist_name as entity_name,
        artist_affinity_score as affinity_score,
        affinity_category,
        raw_score,
        interaction_count,
        last_interaction,
        row_number() over (order by artist_affinity_score desc) as preference_rank,
        spotify_popularity,
        popularity_tier,
        followers,
        followers_tier,
        primary_genre,
        genres,
        genre_count,
        null as artist_count,
        null as from_genre,
        null as to_genre,
        null as transition_count
    from artist_preferences

union all

-- Genre preferences ranking 
select 
    'genre' as preference_type,
    genre_name as entity_name,
    genre_affinity_score as affinity_score,
    affinity_category,
    raw_score,
    interaction_count,
    last_interaction,
    row_number() over (order by genre_affinity_score desc) as preference_rank,
    NULL AS spotify_popularity,
    NULL AS popularity_tier,
    NULL AS followers,
    NULL AS followers_tier,
    NULL AS primary_genre,
    NULL AS genres,
    NULL AS genre_count,
    artist_count,
    NULL AS from_genre,
    NULL AS to_genre,
    NULL AS transition_count
FROM genre_preferences
)

-- Final selection
SELECT 
    preference_type,
    entity_name,
    affinity_score,
    affinity_category,
    raw_score,
    interaction_count,
    last_interaction,
    preference_rank,
    spotify_popularity,
    popularity_tier,
    followers,
    followers_tier,
    primary_genre,
    genres,
    genre_count,
    artist_count,
    from_genre,
    to_genre,
    transition_count,
    CURRENT_TIMESTAMP AS calculated_at
FROM final_outputs
ORDER BY preference_type, COALESCE(preference_rank, 9999)
