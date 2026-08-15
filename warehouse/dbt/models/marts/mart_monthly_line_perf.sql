-- Grain: (train_type, line_number) x service_month. Mirrors
-- mart_monthly_station_perf exactly — same cancellation-denominator
-- rule, same delay/on-time measures excluding canceled rows, same
-- eligible_for_ranking floor. line_number = NULL is a valid grain value
-- for long-distance/international train_type values (see dim_train_line)
-- and is never filtered out here — a long-distance train_type still
-- gets its own (train_type, NULL) row per month.

with enriched as (

    select * from {{ ref('int_observations_enriched') }}

),

monthly as (

    select
        train_type,
        line_number,
        date_trunc('month', service_date)::date as service_month,
        station_coverage_era,

        count(*) as observation_count,
        sum(case when is_canceled then 1 else 0 end) as cancellation_count,

        count(case when not is_canceled then 1 end) as non_canceled_observation_count,
        avg(case when not is_canceled then delay_recomputed_min end) as avg_delay_min,
        median(case when not is_canceled then delay_recomputed_min end) as median_delay_min,
        percentile_cont(0.90) within group (order by case when not is_canceled then delay_recomputed_min end) as p90_delay_min,
        percentile_cont(0.95) within group (order by case when not is_canceled then delay_recomputed_min end) as p95_delay_min,
        percentile_cont(0.99) within group (order by case when not is_canceled then delay_recomputed_min end) as p99_delay_min,
        sum(case when not is_canceled and is_on_time then 1 else 0 end) as on_time_count

    from enriched
    group by train_type, line_number, date_trunc('month', service_date)::date, station_coverage_era

)

select
    *,
    cancellation_count::float / nullif(observation_count, 0) as cancellation_rate,
    on_time_count::float / nullif(non_canceled_observation_count, 0) as on_time_rate,
    observation_count >= 30 as eligible_for_ranking
from monthly
