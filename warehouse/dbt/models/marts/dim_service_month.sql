with available_months as (

    select distinct
        service_month::date as service_month

    from {{ ref('mart_monthly_station_perf') }}

    where service_month is not null
)

select
    service_month,

    to_char(service_month, 'YYYY') as service_year,

    extract(month from service_month)::int as month_number,

    to_char(service_month, 'Mon') as month_name,

    to_char(service_month, 'MM - Mon') as month_label

from available_months