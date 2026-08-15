-- Grain: eva x service_month. Full refresh (not incremental) — see
-- mart_daily_station_perf for the incremental version.
--
-- Cancellation-denominator decision: canceled rows count toward
-- observation_count and cancellation_rate, but are excluded from every
-- delay/on-time measure (avg/median/p90/p95/p99 delay, on_time_rate) —
-- canceled trips didn't run, so they have no real "was it late" outcome,
-- and profiling found their delay values run consistently higher, which
-- would skew delay stats upward if included.
--
-- eligible_for_ranking = observation_count >= 30 (minimum-observation
-- floor) is exposed as a column rather than filtered out silently, so a
-- low-volume station-month is visible in the mart but excluded from
-- ranking queries explicitly, not just missing without explanation.
--
-- station_coverage_era is carried through, not enforced here — any
-- trend/comparison query built on this mart must scope itself to a
-- single era; the mart can't prevent a query from spanning both.

with enriched as (

    select * from {{ ref('int_observations_enriched') }}

),

monthly as (

    select
        eva,
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
    group by eva, date_trunc('month', service_date)::date, station_coverage_era

)

select
    *,
    cancellation_count::float / nullif(observation_count, 0) as cancellation_rate,
    on_time_count::float / nullif(non_canceled_observation_count, 0) as on_time_rate,
    observation_count >= 30 as eligible_for_ranking
from monthly
