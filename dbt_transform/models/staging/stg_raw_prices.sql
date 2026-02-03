with source as (
    select * from {{ source('crypto_data', 'raw_prices') }}
)

select
    coin,
    price_usd,
    timestamp as price_timestamp,
    ingested_at
from source