{{
    config(
        materialized='table'
    )
}}

with date_spine as (
    -- 株価データに存在する日付の範囲を取得
    select
        min(date) as min_date,
        max(date) as max_date
    from {{ ref('stg_stock_prices') }}
),

date_range as (
    -- 日付の範囲を生成
    select
        unnest(generate_series(
            (select min_date from date_spine),
            (select max_date from date_spine),
            interval '1 day'
        ))::date as date
),

enriched as (
    select
        date,
        extract(year from date)::int as year,
        extract(quarter from date)::int as quarter,
        extract(month from date)::int as month,
        extract(day from date)::int as day,
        extract(dow from date)::int as day_of_week,
        case extract(dow from date)::int
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as day_name,
        extract(dow from date)::int in (0, 6) as is_weekend,
        -- 日本の会計年度（4月始まり）
        case
            when extract(month from date) >= 4 then extract(year from date)::int
            else extract(year from date)::int - 1
        end as fiscal_year,
        case
            when extract(month from date) between 4 and 6 then 1
            when extract(month from date) between 7 and 9 then 2
            when extract(month from date) between 10 and 12 then 3
            else 4
        end as fiscal_quarter
    from date_range
)

select * from enriched
