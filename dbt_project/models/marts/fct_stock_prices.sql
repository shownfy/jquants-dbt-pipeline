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
        stock_prices.company_code,
        stock_prices.date,
        stock_prices.open_price,
        stock_prices.high_price,
        stock_prices.low_price,
        stock_prices.close_price,
        stock_prices.volume,
        stock_prices.turnover_value,
        stock_prices.adjusted_close
    from stock_prices
    inner join companies
        using (company_code)
)

select * from valid_prices
