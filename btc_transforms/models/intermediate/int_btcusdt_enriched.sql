with base as (
    select * from {{ ref('stg_btcusdt_klines') }}
),

enriched as (
    select
        *,
        -- Price metrics
        high - low                                      as price_range,
        abs([close] - [open])                           as body_size,
        case when [close] >= [open] then 1 else 0 end   as is_bullish,
        (high + low + [close]) / 3.0                    as typical_price,

        -- Return
        ([close] - [open]) / nullif([open], 0) * 100    as candle_return_pct,

        -- Buy pressure
        taker_buy_base / nullif(volume, 0)              as buy_pressure_ratio,

        -- Trading session (UTC)
        case
            when [hour] >= 0  and [hour] < 8  then 'Asian'
            when [hour] >= 8  and [hour] < 13 then 'European'
            when [hour] >= 13 and [hour] < 21 then 'US'
            else 'Off-Hours'
        end                                             as trading_session
    from base
)

select * from enriched
