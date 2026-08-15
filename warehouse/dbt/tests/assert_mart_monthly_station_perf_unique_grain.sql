select eva, service_month, count(*) as row_count
from {{ ref('mart_monthly_station_perf') }}
group by eva, service_month
having count(*) > 1
