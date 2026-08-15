-- Grain: (train_type, line_number). line_number is allowed to be null as
-- a *valid* grain value, not missing data — long-distance/international
-- train_types (ICE, IC, EC, ...) structurally carry no line_number.
-- Keeping them as their own dimension rows (rather than filtering them
-- out) lets line-performance marts answer "do long-distance categories
-- differ in reliability?" from the same table as regional line questions.

select
    train_type,
    line_number
from {{ ref('int_observations_enriched') }}
group by train_type, line_number
