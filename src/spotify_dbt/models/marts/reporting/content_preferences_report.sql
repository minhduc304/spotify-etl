-- cotent_preferences_report.sql
/*
Purpose: User-facing music preference reporting and insights
This model:
- Builds on artist_and_genre_preferences analytical model
- Enhances raw affinity scores with contextual dimensions
- Segments artists by familiarity and listening patterns
- Visualizes genre distribution across listening sessions
- Correlates genres with listening contexts (time of day, weekday/weekend)
- Calculates period-over-period metrics (weekly/monthly changes)
- Highlights emerging genres and trending artists
- Generates user-friendly insights for dashboard presentation

Sources from: artist_and_genre_preferences, stg_spotify_user_listening_history
*/

{{
    config(
        materialized='table',
        schema='reporting',
        tags=['report', 'daily']
    )
}}

-- 1. Start with core artist and genre preferences
with core_preferences as (
    select 
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
        calculated_at
    from {{ ref('artist_and_genre_preferences') }}
),

-- 2. Artist familiarity segmentation
artist_familiarity as (
    select 
        p.entity_name as artist_name,
        p.affinity_score,
        p.affinity_category,
        p.preference_rank,
        p.spotify_popularity,
        p.followers,
        -- Create artist familiarity segments
        case
            when p.affinity_score >= 7.0 and p.spotify_popularity >= 70 then 'mainstream_favorite'
            when p.affinity_score >= 7.0 and p.spotify_popularity < 70 then 'personal_favorite'
            when p.affinity_score >= 4.0 and p.spotify_popularity >= 70 then 'popular_familiar'
            when p.affinity_score >= 4.0 and p.spotify_popularity < 70 then 'niche_familiar'
            when p.affinity_score < 4.0 and p.spotify_popularity >= 70 then 'mainstream_casual'
            else 'niche_discovery'
        end as familiarity_segment
    from core_preferences p
    where p.preference_type = 'artist'
),

-- 3. Recent listening patterns (last 30 days)
recent_listening as (
    select
        h.primary_artist_id,
        count(*) as recent_plays,
        count(distinct date_trunc('day', h.played_at)) as active_days,
        count(distinct h.session_id) as active_sessions,
        avg(h.completion_percentage) as avg_completion_rate,
        sum(case when h.is_completed then 1 else 0 end) as complete_plays,
        sum(case when h.is_skipped then 1 else 0 end) as skipped_plays
    from {{ ref('stg_spotify_user_listening_history') }} h
    where h.played_at >= current_date - interval '30 days'
    group by h.primary_artist_id
),

-- 4. Artist listening trends (current vs previous 30 days)
artist_trends as (
    select
        h.primary_artist_id,
        count(case when h.played_at >= current_date - interval '30 days' then 1 else null end) as current_period_plays,
        count(case when h.played_at >= current_date - interval '60 days' 
                   and h.played_at < current_date - interval '30 days' then 1 else null end) as previous_period_plays,
        -- Calculate percentage change
        case
            when count(case when h.played_at >= current_date - interval '60 days' 
                           and h.played_at < current_date - interval '30 days' then 1 else null end) = 0 then 100.0
            else round(
                (count(case when h.played_at >= current_date - interval '30 days' then 1 else null end)::float / 
                 count(case when h.played_at >= current_date - interval '60 days' 
                           and h.played_at < current_date - interval '30 days' then 1 else null end)::float - 1) * 100, 2
            )
        end as play_change_percentage
    from {{ ref('stg_spotify_user_listening_history') }} h
    where h.played_at >= current_date - interval '60 days'
    group by h.primary_artist_id
),

-- 5. Genre distribution across time of day
genre_time_distribution as (
    select
        h.primary_artist_genre as genre_name,
        h.time_of_day,
        count(*) as play_count,
        -- Calculate percentage of genre plays by time of day
        round(count(*)::float / sum(count(*)) over (partition by h.primary_artist_genre) * 100, 2) as time_distribution_pct
    from {{ ref('stg_spotify_user_listening_history') }} h
    where h.primary_artist_genre is not null
    group by h.primary_artist_genre, h.time_of_day
),

-- 6. Genre distribution across weekday/weekend
genre_day_distribution as (
    select
        h.primary_artist_genre as genre_name,
        h.day_type,
        count(*) as play_count,
        -- Calculate percentage of genre plays by day type
        round(count(*)::float / sum(count(*)) over (partition by h.primary_artist_genre) * 100, 2) as day_distribution_pct
    from {{ ref('stg_spotify_user_listening_history') }} h
    where h.primary_artist_genre is not null
    group by h.primary_artist_genre, h.day_type
),

-- 7. Genre trends (current vs previous 30 days)
genre_trends as (
    select
        h.primary_artist_genre as genre_name,
        count(case when h.played_at >= current_date - interval '30 days' then 1 else null end) as current_period_plays,
        count(case when h.played_at >= current_date - interval '60 days' 
                   and h.played_at < current_date - interval '30 days' then 1 else null end) as previous_period_plays,
        -- Calculate percentage change
        case
            when count(case when h.played_at >= current_date - interval '60 days' 
                           and h.played_at < current_date - interval '30 days' then 1 else null end) = 0 then 100.0
            else round(
                (count(case when h.played_at >= current_date - interval '30 days' then 1 else null end)::float / 
                 count(case when h.played_at >= current_date - interval '60 days' 
                           and h.played_at < current_date - interval '30 days' then 1 else null end)::float - 1) * 100, 2
            )
        end as play_change_percentage
    from {{ ref('stg_spotify_user_listening_history') }} h
    where h.primary_artist_genre is not null
    and h.played_at >= current_date - interval '60 days'
    group by h.primary_artist_genre
),

-- 8. Emerging genres (low historical plays but increasing trend)
emerging_genres as (
    select
        gt.genre_name,
        cp.affinity_score,
        gt.current_period_plays,
        gt.previous_period_plays,
        gt.play_change_percentage,
        -- Identify emerging genres as those with large percentage increases
        case
            when gt.previous_period_plays <= 5 
            and gt.current_period_plays >= 10 
            and gt.play_change_percentage >= 100.0 
            then true
            else false
        end as is_emerging
    from genre_trends gt
    left join core_preferences cp on gt.genre_name = cp.entity_name and cp.preference_type = 'genre'
    where gt.play_change_percentage > 0
),

-- 9. Monthly listening summary (last 3 months)
monthly_summary as (
    select
        date_trunc('month', h.played_at) as month,
        count(*) as total_plays,
        count(distinct h.primary_artist_id) as unique_artists,
        count(distinct h.primary_artist_genre) as unique_genres,
        count(distinct h.session_id) as listening_sessions,
        round(avg(h.completion_percentage), 2) as avg_completion_percentage,
        sum(case when h.is_completed then 1 else 0 end)::float / count(*) * 100 as completion_rate,
        sum(case when h.is_skipped then 1 else 0 end)::float / count(*) * 100 as skip_rate
    from {{ ref('stg_spotify_user_listening_history') }} h
    where h.played_at >= current_date - interval '3 months'
    group by date_trunc('month', h.played_at)
),

-- 10. Artist enriched with trends and listening patterns
artist_enriched as (
    select
        cp.entity_name as artist_name,
        cp.affinity_score,
        cp.affinity_category,
        cp.preference_rank,
        cp.spotify_popularity,
        cp.popularity_tier,
        cp.followers,
        cp.followers_tier,
        cp.primary_genre,
        cp.genres,
        af.familiarity_segment,
        rl.recent_plays,
        rl.active_days,
        rl.active_sessions,
        rl.avg_completion_rate,
        rl.complete_plays,
        rl.skipped_plays,
        at.current_period_plays,
        at.previous_period_plays,
        at.play_change_percentage,
        -- Trending status
        case
            when at.play_change_percentage >= 50 and rl.recent_plays >= 5 then 'trending_up'
            when at.play_change_percentage <= -50 and at.previous_period_plays >= 5 then 'trending_down'
            when at.play_change_percentage between -10 and 10 then 'stable'
            when at.play_change_percentage > 10 then 'increasing'
            when at.play_change_percentage < -10 then 'decreasing'
            else 'insufficient_data'
        end as trending_status
    from core_preferences cp
    left join artist_familiarity af on cp.entity_name = af.artist_name
    left join recent_listening rl on cp.primary_artist_id = rl.primary_artist_id
    left join artist_trends at on cp.primary_artist_id = at.primary_artist_id
    where cp.preference_type = 'artist'
),

-- 11. Genre enriched with context and trends
genre_enriched as (
    select
        cp.entity_name as genre_name,
        cp.affinity_score,
        cp.affinity_category,
        cp.preference_rank,
        cp.artist_count,
        gt.current_period_plays,
        gt.previous_period_plays,
        gt.play_change_percentage,
        eg.is_emerging,
        -- Trending status
        case
            when gt.play_change_percentage >= 50 and gt.current_period_plays >= 5 then 'trending_up'
            when gt.play_change_percentage <= -50 and gt.previous_period_plays >= 5 then 'trending_down'
            when gt.play_change_percentage between -10 and 10 then 'stable'
            when gt.play_change_percentage > 10 then 'increasing'
            when gt.play_change_percentage < -10 then 'decreasing'
            else 'insufficient_data'
        end as trending_status
    from core_preferences cp
    left join genre_trends gt on cp.entity_name = gt.genre_name
    left join emerging_genres eg on cp.entity_name = eg.genre_name
    where cp.preference_type = 'genre'
),

-- 12. Time of day preferences for genres
genre_time_preferences as (
    select
        gtd.genre_name,
        max(case when gtd.time_of_day = 'morning' then gtd.time_distribution_pct else 0 end) as morning_pct,
        max(case when gtd.time_of_day = 'afternoon' then gtd.time_distribution_pct else 0 end) as afternoon_pct,
        max(case when gtd.time_of_day = 'evening' then gtd.time_distribution_pct else 0 end) as evening_pct,
        max(case when gtd.time_of_day = 'night' then gtd.time_distribution_pct else 0 end) as night_pct,
        -- Preferred time of day
        (array_position(
            array_agg(gtd.time_distribution_pct order by gtd.time_distribution_pct desc),
            max(gtd.time_distribution_pct)
        ))[1] as preferred_time
    from genre_time_distribution gtd
    group by gtd.genre_name
),

-- 13. Day type preferences for genres
genre_day_preferences as (
    select
        gdd.genre_name,
        max(case when gdd.day_type = 'weekday' then gdd.day_distribution_pct else 0 end) as weekday_pct,
        max(case when gdd.day_type = 'weekend' then gdd.day_distribution_pct else 0 end) as weekend_pct,
        -- Preferred day type
        case
            when max(case when gdd.day_type = 'weekday' then gdd.day_distribution_pct else 0 end) >
                 max(case when gdd.day_type = 'weekend' then gdd.day_distribution_pct else 0 end)
            then 'weekday'
            else 'weekend'
        end as preferred_day_type
    from genre_day_distribution gdd
    group by gdd.genre_name
),

-- 14. User insights generation
user_insights as (
    select
        'Top genres across time of day' as insight_type,
        time_of_day as context,
        array_to_string(
            array(
                select genre_name
                from genre_time_distribution
                where time_of_day = gtd.time_of_day
                order by play_count desc
                limit 3
            ),
            ', '
        ) as insight_value
    from genre_time_distribution gtd
    group by time_of_day
    
    union all
    
    select
        'Emerging genres' as insight_type,
        'Last 30 days' as context,
        array_to_string(
            array(
                select genre_name
                from emerging_genres
                where is_emerging = true
                order by current_period_plays desc
                limit 5
            ),
            ', '
        ) as insight_value
    
    union all
    
    select
        'Top trending artists' as insight_type,
        'Last 30 days' as context,
        array_to_string(
            array(
                select artist_name
                from artist_enriched
                where trending_status = 'trending_up'
                order by play_change_percentage desc
                limit 5
            ),
            ', '
        ) as insight_value
    
    union all
    
    select
        'Most consistent artists' as insight_type,
        'Last 30 days' as context,
        array_to_string(
            array(
                select artist_name
                from artist_enriched
                where active_days >= 15  -- Active for at least half the month
                order by active_days desc
                limit 5
            ),
            ', '
        ) as insight_value
)

-- Final model output: Artist preferences with enrichment
select
    'artist_preferences' as report_section,
    ae.artist_name as entity_name,
    ae.affinity_score,
    ae.affinity_category,
    ae.preference_rank,
    ae.spotify_popularity,
    ae.popularity_tier,
    ae.followers,
    ae.followers_tier,
    ae.primary_genre,
    ae.genres,
    ae.familiarity_segment,
    -- Listening stats
    ae.recent_plays,
    ae.active_days,
    ae.active_sessions,
    ae.avg_completion_rate,
    ae.complete_plays,
    ae.skipped_plays,
    -- Trend metrics
    ae.current_period_plays,
    ae.previous_period_plays,
    ae.play_change_percentage,
    ae.trending_status,
    -- Empty fields for genre-specific columns
    null as morning_pct,
    null as afternoon_pct,
    null as evening_pct,
    null as night_pct,
    null as preferred_time,
    null as weekday_pct,
    null as weekend_pct,
    null as preferred_day_type,
    null as is_emerging,
    -- Metadata
    current_timestamp as generated_at
from artist_enriched ae

union all

-- Genre preferences with enrichment
select
    'genre_preferences' as report_section,
    ge.genre_name as entity_name,
    ge.affinity_score,
    ge.affinity_category,
    ge.preference_rank,
    null as spotify_popularity,
    null as popularity_tier,
    null as followers,
    null as followers_tier,
    null as primary_genre,
    null as genres,
    null as familiarity_segment,
    -- Listening stats
    ge.current_period_plays as recent_plays,
    null as active_days,
    null as active_sessions,
    null as avg_completion_rate,
    null as complete_plays,
    null as skipped_plays,
    -- Trend metrics
    ge.current_period_plays,
    ge.previous_period_plays,
    ge.play_change_percentage,
    ge.trending_status,
    -- Time context
    gtp.morning_pct,
    gtp.afternoon_pct,
    gtp.evening_pct,
    gtp.night_pct,
    gtp.preferred_time,
    gdp.weekday_pct,
    gdp.weekend_pct,
    gdp.preferred_day_type,
    ge.is_emerging,
    -- Metadata
    current_timestamp as generated_at
from genre_enriched ge
left join genre_time_preferences gtp on ge.genre_name = gtp.genre_name
left join genre_day_preferences gdp on ge.genre_name = gdp.genre_name

union all

-- Monthly summary section
select
    'monthly_summary' as report_section,
    to_char(ms.month, 'YYYY-MM') as entity_name,
    null as affinity_score,
    null as affinity_category,
    null as preference_rank,
    null as spotify_popularity,
    null as popularity_tier,
    null as followers,
    null as followers_tier,
    null as primary_genre,
    null as genres,
    null as familiarity_segment,
    -- Listening stats
    ms.total_plays as recent_plays,
    null as active_days,
    ms.listening_sessions as active_sessions,
    ms.avg_completion_percentage as avg_completion_rate,
    null as complete_plays,
    null as skipped_plays,
    -- Summary metrics (reuse trend fields)
    ms.unique_artists as current_period_plays,
    ms.unique_genres as previous_period_plays,
    ms.completion_rate as play_change_percentage,
    null as trending_status,
    -- Empty fields for genre-specific columns
    null as morning_pct,
    null as afternoon_pct,
    null as evening_pct,
    null as night_pct,
    null as preferred_time,
    null as weekday_pct,
    null as weekend_pct,
    null as preferred_day_type,
    null as is_emerging,
    -- Metadata
    current_timestamp as generated_at
from monthly_summary ms

union all

-- User insights section
select
    'user_insights' as report_section,
    ui.insight_type as entity_name,
    null as affinity_score,
    null as affinity_category,
    null as preference_rank,
    null as spotify_popularity,
    null as popularity_tier,
    null as followers,
    null as followers_tier,
    null as primary_genre,
    null as genres,
    null as familiarity_segment,
    -- Insights in listening stats field
    null as recent_plays,
    null as active_days,
    null as active_sessions,
    null as avg_completion_rate,
    null as complete_plays,
    null as skipped_plays,
    -- Context and value in trend fields
    null as current_period_plays,
    null as previous_period_plays,
    null as play_change_percentage,
    null as trending_status,
    -- Empty fields for genre-specific columns
    null as morning_pct,
    null as afternoon_pct,
    null as evening_pct,
    null as night_pct,
    null as preferred_time,
    null as weekday_pct,
    null as weekend_pct,
    null as preferred_day_type,
    null as is_emerging,
    -- Store insights in generated_at field
    ui.context || ': ' || ui.insight_value as generated_at
from user_insights ui

order by
    report_section,
    coalesce(preference_rank, 9999)