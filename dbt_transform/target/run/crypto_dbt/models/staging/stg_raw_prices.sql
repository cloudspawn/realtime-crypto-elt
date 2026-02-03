

  create or replace view `bigquery-486314`.`crypto_data`.`stg_raw_prices`
  OPTIONS()
  as with source as (
    select * from `bigquery-486314`.`crypto_data`.`raw_prices`
)

select
    coin,
    price_usd,
    timestamp as price_timestamp,
    ingested_at
from source;

