-- Keyed on eva, not station_name: only 5 of ~5,400 EVA codes show any
-- name instability, and even those resolve to one stable station_name
-- each. xml_station_name_variant_count is a diagnostic only (the ~28%
-- row-level station_name/xml_station_name mismatch is a known, accepted
-- pattern, resolved by choosing station_name as canonical upstream in
-- int_observations_enriched — this count just makes that variation
-- visible per station, not a data-quality flag on its own).
--
-- station_name falls back to xml_station_name when the canonical field
-- never resolved: 4 EVA codes (~1,522 rows in the loaded window) have a
-- null station_name but a usable xml_station_name.

select
    eva,
    coalesce(max(station_name), max(xml_station_name)) as station_name,
    count(distinct xml_station_name) as xml_station_name_variant_count
from {{ ref('int_observations_enriched') }}
group by eva
