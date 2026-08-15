-- dim_train_line's grain is (train_type, line_number), with line_number
-- allowed to be null as a valid value (not the same as missing). A
-- generic `unique` test on a single column can't express this grain, and
-- standard equality-based grouping already treats NULL = NULL as one
-- group in GROUP BY, so this is really just a sanity check that the
-- model's own GROUP BY did what it should.

select train_type, line_number, count(*) as row_count
from {{ ref('dim_train_line') }}
group by train_type, line_number
having count(*) > 1
