{{ config(materialized='table') }}

-- Business grain is (train_line_ride_id, train_line_station_num,
-- source_month), not id: the source is poll-and-snapshot, and the same
-- ride-stop is re-captured repeatedly as its delay/timestamps/cancellation
-- status evolve (up to 35 times per key in profiling). Keep the most
-- recent snapshot per key; id cannot be the primary ordering key because
-- 100% of duplicate groups span multiple ids with no recency signal — it
-- is only used as a secondary tiebreak for the rare case of two rows
-- sharing an identical "time".

with unioned as (

    select * from {{ ref('int_observations_unioned') }}

),

ranked as (

    select
        *,
        row_number() over (
            partition by train_line_ride_id, train_line_station_num, source_month
            order by "time" desc, id desc
        ) as observation_rank

    from unioned

)

select
    id,
    source_month,
    ingestion_source,

    station_name,
    xml_station_name,
    eva,
    final_destination_station,

    train_number,
    line_number,
    train_type,
    train_line_ride_id,
    train_line_station_num,

    delay_in_min,
    is_canceled,

    "time",
    arrival_planned_time,
    arrival_change_time,
    departure_planned_time,
    departure_change_time

from ranked
where observation_rank = 1
