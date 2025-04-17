-- macros/calculate_affinity_scores.sql
{% macro calculate_affinity_scores(
    table_name.
    entity_id_column,
    timestamp_column,
    event_type_column=none,
    event_weights={'listen': 1.0, 'save': 3.0, 'playlist_add': 2.5},
    time_decay_days=90,
    normalization_method='minmax',
    min_score=0,
    max_score=10
) %}

/*
Purpose: Standardized approach to calculating user affinities
This macro:
- Implements a weighted scoring algorithm for user preferences
- Normalizes scores across different dimensions
- Handles frequency decay for historical items
- Provides consistent methodology across models

Parameters: 
    table_name (string): Source table containing interaction data
    entity_id_column (string): Column for the entity to calculate affinity for (track_id, artist_id, etc.)
    timestamp_column (string): Column for the timestamp of the interaction
    event_type_column (string, optional): Column for the type of interaction event
    event_weights (dict, optional): Number of days for full decay weight (older = less weight)
    normalization_method (string, optional): Method to score normalization ('minmax', 'zscore', 'percentile')
    min_score: (int, optional): Minimum value for normalized scores
    max_score (int, optional): Maximum value for normalized scores
*/  

{# Calculate time decay factor based one days since event #}
{%- set time_decay_sql -%}
    case 
        when {{ timestamp_column }} is null then 0
        else (1.0 / (1.0  + extract(day from(current_timestamp - {{ timestamp_column }}))/ {{ time_decay_days }}))
    end
{%- endset -%}

{# Determine event weights based on event type if provided #}
{%- if event_type_column is not none -%}
    {%- set event_weight_sql -%}
        case 
            {% for event_type, weight in event_weights.items() %}
            when {{ event_type_column }} = '{{ event_type }}' then {{ weight }}
            {% endfor %}
            else 1.0
        end 
    {%- endset -%}
{%- else -%}
    {%- set event_weight_sql -%} 1.0 {%- endset -%}

{# Build the raw affinity score calculations #}
with raw_affinity as (
    select 
        {{entity_id_column}},
        sum({{ time_decay_sql }} *  {{ event_weight_sql }}) as raw_score,
        count(*) as interaction_count,
        max({{ timestamp_column }}) as last_interaction,
    from {{ table_name }}
    where {{ entity_id_column }} is not null 
    group by {{ entity_id_column }}
),

{# Calculate statistics for normalization #}
stats as (
    select 
        max(raw_score) as max_score,
        min(raw_score) as min_score,
        avg(raw_score) as avg_score,
        stdev(raw_score) as stdev_score,
        percentile_cont(0.5) within group (order by raw_score) as median_score
    from raw_affinity
),

{# Apply normalization based on selected method #}
normalized_affinity as (
    select 
        r.*,
        case 
            {# Min-Max scaling #}
            when '{{ normalization_method }}' = 'minmax' then 
                case 
                    when s.max_score = s.min_score then  {{ min_score }}
                    else {{ min_score }} + ((r.raw_score - s.min_score) / (s.max_score - s.min_score)) * ({{ max_score }} - {{ min_score }})
                end 
            {# Z-score normalization #}
            when '{{ normalization_method }}' = '{{ zscore }}' then 
                case 
                    when s.stdev_score = 0 then {{ min_score }}
                    else greatest(least( {{ min_score }} + 5 + ((r.raw_score - s.avg_score) s.stdev_score) * 2, {{ max_score }}), {{ min_score }})
                end 
            {# Percentile-based scoring #}
            when '{{ normalization_method }}' = 'percentile' then
                percent_rank() over (order by r.raw_score) * ({{ max_score }} - {{ min_score }}) + {{ min_score }}
            {# Default to raw score if invalid method #}
            else r.raw_score
        end as normalized_score
    from raw_affinity r
    cross join stats s 
)

select 
    {{ entity_id_column }},
    raw_score,
    normalized_score,
    intertaction_count,
    last_interaction,
    current_timestamp as calculated_at
from normalized_affinity
order by normalized_score DESC


