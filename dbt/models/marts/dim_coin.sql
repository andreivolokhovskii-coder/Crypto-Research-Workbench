{{
    config(
        materialized='table',
        engine='ReplacingMergeTree(updated_at)',
        order_by='(coin_id)',
        settings={'index_granularity': 8192}
    )
}}

-- Latest snapshot per coin from silver_coin_metadata
select
    coin_id,
    symbol,
    name,
    market_cap_rank,
    category,
    max(snapshot_date)  as updated_at
from {{ source('crypto', 'silver_coin_metadata') }}
group by coin_id, symbol, name, market_cap_rank, category
