{{
    config(
        materialized='table',
        engine='ReplacingMergeTree(updated_at)',
        order_by='(coin_id)',
        settings={'index_granularity': 8192}
    )
}}

-- Latest snapshot per coin from silver_coin_metadata.
-- argMax picks the column value from the row with the highest snapshot_date,
-- guaranteeing exactly one row per coin_id regardless of schema changes or
-- re-ingestion of old snapshots with different symbol/rank/category values.
select
    coin_id,
    argMax(symbol,          snapshot_date)  as symbol,
    argMax(name,            snapshot_date)  as name,
    argMax(market_cap_rank, snapshot_date)  as market_cap_rank,
    argMax(category,        snapshot_date)  as category,
    max(snapshot_date)                      as updated_at
from {{ source('crypto', 'silver_coin_metadata') }} FINAL
group by coin_id
