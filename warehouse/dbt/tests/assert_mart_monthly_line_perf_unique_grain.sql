select train_type, line_number, service_month, count(*) as row_count
from {{ ref('mart_monthly_line_perf') }}
group by train_type, line_number, service_month
having count(*) > 1
