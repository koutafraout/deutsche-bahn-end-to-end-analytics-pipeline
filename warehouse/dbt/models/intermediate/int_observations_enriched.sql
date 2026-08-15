{{ config(materialized='table') }}

-- Adds shared semantic fields on top of int_observations_deduped:
-- recomputed delay (departure-authoritative, arrival-fallback), on-time
-- flag, service date/hour/weekday, and station_coverage_era.

with deduped as (

    select * from {{ ref('int_observations_deduped') }}

),

event_resolved as (

    select
        *,

        -- Departure-priority-else-arrival: matches the same pattern the
        -- source's own "time" column already follows (§8.1). Applied here
        -- to delay recomputation so both stay consistent.
        case
            when departure_planned_time is not null
                and departure_change_time is not null
                then 'departure'
            else 'arrival'
        end as delay_basis

    from deduped

),

enriched as (

    select
        *,

        case
            when delay_basis = 'departure' then
                datediff(minute, departure_planned_time, departure_change_time)
            else
                datediff(minute, arrival_planned_time, arrival_change_time)
        end as delay_recomputed_min,

        date("time") as service_date,
        extract(hour from "time") as service_hour,
        extract(dow from "time") as service_weekday_num,
        to_char("time", 'Dy') as service_weekday_name,
        to_char("time", 'Dy') in ('Sat', 'Sun') as is_weekend,

        case
            when date("time") < date('2025-11-02') then 'limited_coverage'
            else 'full_coverage'
        end as station_coverage_era

    from event_resolved

)

select
    *,
    delay_recomputed_min < 6 as is_on_time
from enriched
