-- listening_activity_report.sql
/*
Purpose: Provide day-level summary metrics for dashboards
This model:
- Creates daily metrics for total listening time
- Counts unique tracks and artists per day
- Shows peak listening hours
- Compares weekday vs weekend listening patterns
- Provides trend indicators (up/down vs previous day/week)
- Measures listening consistency and routines
- Calculates discovery rates for new music
- Identifies listening habit changes
- Source from stg_spotify_user_listening_history

*/

{{
    config(
        materialized='view',
        schema='reporting',
        tags=['report', 'daily']
    )
}}

with week_listening_patterns as (
    seelct 
)

with daily_listening as (
    select 
        date_trunc('day', played_at) as listening_date,
        day_type,
        count(*) as total_plays,
        count(distinct track_id) as unique_tracks,
        count(primary_artist_id) as unique_artists,
        sum(ms_played) / 3600000.0 as total_hours_played,
        sum(case when is_completed then 1 else 0) as completed_plays,
        sum(case when is_skipped then 1 else 0) as skipped_plays,
        round(avg(completion_percentage), 2) as avg_completetion_percentage
    from {{ ref('stg_spotify_user_listening_history') }}
    group by 1, 2
),

hourly_patterns as (
    select 
        date_trunc('day', played_at) as listening_date,
        hour_of_day,
        count(*) as plays_in_hour,
        sum(ms_played) / 60000.0 as minutes_played,
    from {{ ref('stg_spotify_user_listening_history') }}
),

peak_hours as (
    select 
        listening_date,
        hour_of_day,
        plays_in_hour,
        minutes_played,
    from (
        select 
            listening_date,
            hour_of_day,
            plays_in_hour,
            minutes_played,
            row_number() over (partition by listening_date order by plays_in_hour DESC) as hour_rank
        from hourly_patterns
    ) ranked 
    where hour_rank = 1
),

time_of_day_distribution as (
    select 
        date_trunc('day', played_at) as listening_date,
        time_of_day,
        count(*) as plays,
        sum(ms_played) / 3600000.0 as hours_played,
        round(100.0 * count(*) / sum(count(*)) over (partition by date_trunc('day', played_at)), 2) as percentage_daily_plays
    from {{ ref('stg_spotify_user_listening_history') }}
    group by 1, 2
),

-- Define first appearance of tracks to calculate discovery metrics
track_first_played as (
    select 
        track_id,
        min(date_trunc('day', played_at)) as first_played_date
    from {{ ref('stg_spotify_user_listening_history') }}
    group by 1
),

daily_discoveries as (
    select 
        date_trunc('day', h.played_at) as listening_date,
        count(distinct case when date_trunct('day', h.played_at) = f.first_played_date then h.track_id else null end) as new_tracks_count,
        count(distinct h.track_id) as total_tracks_played
    from {{ ref('stg_spotify_user_listening_history') }}
    join track_first_played f on h.track_id = f.track_id
    group by 1
),

-- Calculate trend indicators 
daily_trends as (
    select 
        current_day.listening_date,
        current.day.total_hours_played,
        current_day.total_hours_played - prev_day.total_hours_played as daily_hours_change,
        case 
            when current_day.total_hours_played > prev_day.total_hours_played then 'up'
            when current_day.total_hours_played < prev_day.total_hours_played then 'down'
            else 'same'
        end as daily_trend,
        current_day.total_hours_played - prev_week.total_hours_played as weekly_hours_change,
        case 
            when current_day.total_hours_played > prev_week.total_hours_played then 'up'
            when current_day.total_hours_played < prev_week.total_hours_played then 'down'
            else 'same'
        end as weekly_trend
    from daily_listening current_day
    left join daily_listening prev_day on current_day.listening_date = prev_day.listening_date + interval '1 day'
    left join daily_listening prev_week on current_day.listening_date = prev_week.listening_date + interval '7 days'
),

-- Weekly listening patterns to identify routines
week_listening_patterns as (
    select 
        date_trunc('week', played_at) as week_start,
        day_of_week,
        count(distinct date_trunc('day', played_at)) as active_days,
        count(*) as total_plays,
        sum(ms_played) / 3600000.0 as total_hours_played
    from {{ ref('stg_spotify_user_listening_history') }}
    group by 1, 2
),

-- Listening consistency metrics
consistency metrics as (
    select 
        listening_date,
        -- Count distinct days listened in the past 7 days
        (
            select count(distinct date_trunc('day', inner_hist.played_at))
            from {{ ref('stg_spotify_user_listening_history') }} inner_hist
            where date_trunc('day', inner_hist.played_at) between daily.listening_date - interval '6 days' and daily.listening_date 
        ) as active_days_last_7,
        -- Standard deviation of daily listening hours in past 7 days to measure routine stability
        (
            select stddev(inner_daily.total_hours_played)
            from daily_listening inner_daily
            where inner_daily.listening_date between daily.listening_date - interval '6 days' and daily.listening_date
        ) as listening_time_stability
    from daily_listening daily
)

-- Final report combining all metrics 
select 
    dl.listening_date,
    dl.day_type,
    dl.total_plays,
    dl.unique_tracks,
    dl.unique_artists,
    dl.total_hours_played,
    dl.completed_plays,
    dl.skipped_plays,
    dl.avg_completion_percentage,
    -- Listening quality
    round(dl.completed_plays::float / nullif(dl.total_plays, 0) * 100, 2) as completion_rate,
    -- Peak listening hour
    ph.peak_hour,
    ph.minutes_played as peak_hour_minutes,
    -- Discovery metrics
    dd.new_tracks_count,
    round(dd.new_tracks_count::float / nullif(dd.total_tracks_played, 0) * 100, 2) as discovery_rate,
    -- Trend indicators
    dt.daily_hours_change,
    dt.daily_trend,
    dt.weekly_hours_change,
    dt.weekly_trend,
    -- Consistency metrics
    cm.active_days_last_7,
    cm.listening_time_stability,
    -- Most active time of day
    (
        select time_of_day
        from time_of_day_distribution tod
        where tod.listening_date = dl.listening_date
        order by tod.hours_played desc
        limit 1
    ) as most_active_time_of_day
from daily_listening dl
left join peak_hours ph on dl.listening_date = ph.listening_date
left join daily_discoveries dd on dl.listening_date = dd.listening_date
left join daily_trends dt on dl.listening_date = dt.listening_date
left join consistency_metrics cm on dl.listening_date = cm.listening_date
order by dl.listening_date desc



