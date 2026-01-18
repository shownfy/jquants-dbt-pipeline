{{
    config(
        materialized='table'
    )
}}

with stock_prices as (
    select * from {{ ref('stg_stock_prices') }}
),

companies as (
    select company_code from {{ ref('dim_company') }}
),

-- 存在する会社コードのみを残す
valid_prices as (
    select
        sp.company_code,
        sp.date,
        sp.open_price,
        sp.high_price,
        sp.low_price,
        sp.close_price,
        sp.volume,
        sp.turnover_value,
        sp.adjusted_close
    from stock_prices sp
    inner join companies c on sp.company_code = c.company_code
)

select * from valid_prices


