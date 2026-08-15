with source as (

    select * from {{ source('db_monthly', 'raw_observations') }}

),

renamed as (

    select
        id::varchar as id,
        source_month::varchar as source_month,

        station_name::varchar as station_name,
        xml_station_name::varchar as xml_station_name,
        eva::varchar as eva,
        final_destination_station::varchar as final_destination_station,

        train_number::varchar as train_number,
        line_number::varchar as line_number,
        train_type::varchar as train_type,
        train_line_ride_id::varchar as train_line_ride_id,
        train_line_station_num::integer as train_line_station_num,

        delay_in_min::integer as delay_in_min,
        is_canceled::boolean as is_canceled,

        -- source columns land as raw nanosecond-since-epoch bigints, not
        -- Redshift timestamps; decode via fractional seconds (ns / 1e9)
        -- added to epoch. Redshift's interval literals don't support a
        -- microsecond unit, so seconds is the finest granularity available.
        -- Values represent Europe/Berlin wall-clock time with no tz info,
        -- so the naive (tz-less) result is intentional here. round() the
        -- fractional-seconds division before applying the interval:
        -- floating-point division of nanoseconds by 1e9 occasionally lands
        -- just under a whole second (e.g. 23:33:59.999999 instead of
        -- 23:34:00), which is a decode artifact, not real sub-second data
        -- (profiling confirmed source timestamps are whole-minute
        -- granularity) — but it silently undercounts minute-boundary-based
        -- calculations like datediff(minute, ...) downstream if left
        -- unrounded.
        timestamp 'epoch' + round("time"::bigint / 1000000000.0) * interval '1 second' as "time",
        timestamp 'epoch' + round(arrival_planned_time::bigint / 1000000000.0) * interval '1 second' as arrival_planned_time,
        timestamp 'epoch' + round(arrival_change_time::bigint / 1000000000.0) * interval '1 second' as arrival_change_time,
        timestamp 'epoch' + round(departure_planned_time::bigint / 1000000000.0) * interval '1 second' as departure_planned_time,
        timestamp 'epoch' + round(departure_change_time::bigint / 1000000000.0) * interval '1 second' as departure_change_time

    from source

)

select * from renamed
