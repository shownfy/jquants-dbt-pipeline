-- Staging model for stock prices (J-Quants API V2)
with source as (
    select * from {{ source('raw', 'stock_prices') }}
),

renamed as (
    select
        "Code" as company_code,
        cast("Date" as date) as date,
        "O" as open_price,
        "H" as high_price,
        "L" as low_price,
        "C" as close_price,
        "Vo" as volume,
        "Va" as turnover_value,
        "AdjFactor" as adjustment_factor,
        "AdjO" as adjusted_open,
        "AdjH" as adjusted_high,
        "AdjL" as adjusted_low,
        "AdjC" as adjusted_close
    from source
)

select * from renamed
