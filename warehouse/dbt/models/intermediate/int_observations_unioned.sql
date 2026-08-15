-- stg_api_observations does not exist yet (raw API path not built). Once
-- it does, UNION ALL a second CTE selecting from it with
-- ingestion_source = 'api', referenced via ref() same as monthly below.
-- (Not written as a real Jinja ref() here even in a comment: dbt renders
-- Jinja before SQL comments are stripped, so a commented-out ref() still
-- breaks the DAG if the target model doesn't exist.)

with monthly as (

    select
        *,
        'monthly' as ingestion_source
    from {{ ref('stg_monthly_observations') }}

)

select * from monthly
