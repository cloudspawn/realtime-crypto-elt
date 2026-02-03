with stats as (
    select * from {{ ref('int_prices_with_stats') }}
),

latest as (
    select
        coin,
        price_usd as current_price,
        price_avg_10,
        price_min_all,
        price_max_all,
        price_timestamp,
        row_number() over (partition by coin order by price_timestamp desc) as rn
    from stats
)

select
    coin,
    current_price,
    price_avg_10,
    price_min_all,
    price_max_all,
    round((current_price - price_avg_10) / price_avg_10 * 100, 2) as pct_vs_avg,
    price_timestamp as last_update
from latest
where rn = 1