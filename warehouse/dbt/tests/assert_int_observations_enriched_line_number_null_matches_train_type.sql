-- line_number is null iff train_type is a long-distance/international
-- category — a blanket not_null would be wrong (100% null for these
-- types is expected), and a blanket nullable would miss a real
-- regional-type data-quality signal. Returns rows on either direction of
-- mismatch.
--
-- severity=warn: the long-distance direction (these types must be null)
-- holds with zero exceptions. The regional direction (these types must
-- not be null) has ~0.02% of rows as genuine sparse nulls scattered
-- across several regional types (plus one type, EUR, that is 100% null
-- and belongs in the long-distance list below, already added) — real,
-- expected data messiness, not a systematic bug, so it's surfaced every
-- build without blocking it.
{{ config(severity='warn') }}

{% set long_distance_types = [
    'ICE', 'IC', 'RJ', 'NJ', 'EC', 'ECE', 'EN', 'WB', 'TGV', 'IR', 'KD',
    'TER', 'SDG', 'EST', 'SOE', 'LE', 'ES', 'D', 'GV', 'UEX', 'Sp',
    'DBK', 'LEO', 'ÖBA', 'MSM', 'UEF', 'EUR'
] %}

select *
from {{ ref('int_observations_enriched') }}
where
    (train_type in ({{ "'" ~ long_distance_types | join("','") ~ "'" }}) and line_number is not null)
    or
    (train_type not in ({{ "'" ~ long_distance_types | join("','") ~ "'" }}) and line_number is null)
