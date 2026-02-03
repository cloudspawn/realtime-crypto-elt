

  create or replace view `bigquery-486314`.`crypto_data`.`int_prices_with_stats`
  OPTIONS()
  as with prices as (
    select * from `bigquery-486314`.`crypto_data`.`stg_raw_prices`
),

price_stats as (
    select
        coin,
        price_usd,
        price_timestamp,
        avg(price_usd) over (partition by coin order by price_timestamp rows between 9 preceding and current row) as price_avg_10,
        min(price_usd) over (partition by coin) as price_min_all,
        max(price_usd) over (partition by coin) as price_max_all
    from prices
)

select * from price_stats;

