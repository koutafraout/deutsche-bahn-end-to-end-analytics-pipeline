-- Delay sanity bounds from observed extremes in profiling:
-- [-1446, 1439] minutes. Wide enough to admit real observed outliers (up
-- to ~24h delays occur), narrow enough to catch a genuinely corrupt/
-- overflowed value.

select *
from {{ ref('int_observations_enriched') }}
where delay_recomputed_min < -1446
   or delay_recomputed_min > 1439
