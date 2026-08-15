-- int_observations_deduped's business grain is
-- (train_line_ride_id, train_line_station_num, source_month) — not id.
-- This fails (returns rows) if the dedup window function in the model
-- ever regresses and lets a duplicate grain group through.

select
    train_line_ride_id,
    train_line_station_num,
    source_month,
    count(*) as row_count
from {{ ref('int_observations_deduped') }}
group by train_line_ride_id, train_line_station_num, source_month
having count(*) > 1
