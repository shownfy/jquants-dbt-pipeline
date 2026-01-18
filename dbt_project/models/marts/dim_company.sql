{{
    config(
        materialized='table'
    )
}}

with listed_info as (
    select * from {{ ref('stg_listed_info') }}
)

select
    company_code,
    company_name,
    company_name_english,
    sector_17_code,
    sector_17_name,
    sector_33_code,
    sector_33_name,
    scale_category,
    market_code,
    market_name
from listed_info
