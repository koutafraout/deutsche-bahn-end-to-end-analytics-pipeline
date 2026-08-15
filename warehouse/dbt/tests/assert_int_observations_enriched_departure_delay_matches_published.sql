-- Departure-based delay recomputation matches the source's published
-- delay_in_min 100% of the time in every profiled month. Scoped to
-- delay_basis = 'departure' only — arrival-based recompute matches only
-- ~70% and is documented as non-authoritative, so it is not asserted
-- here. A future failure here is a real regression, not noise.

select *
from {{ ref('int_observations_enriched') }}
where delay_basis = 'departure'
  and delay_recomputed_min != delay_in_min
