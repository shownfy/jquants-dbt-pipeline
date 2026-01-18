-- Staging model for listed company information (J-Quants API V2)
with source as (
    select * from {{ source('raw', 'listed_info') }}
),

renamed as (
    select
        "Code" as company_code,
        "CoName" as company_name,
        "CoNameEn" as company_name_english,
        "S17" as sector_17_code,
        "S17Nm" as sector_17_name,
        "S33" as sector_33_code,
        "S33Nm" as sector_33_name,
        "ScaleCat" as scale_category,
        "Mkt" as market_code,
        "MktNm" as market_name,
        "Date" as data_date
    from source
)

select * from renamed
