with base as (
    select * from {{ ref('int_btcusdt_enriched') }}
),

hourly as (
    select
        hour,
        trading_session,
        count(*)                                        as total_candles,
        avg(candle_return_pct)                          as avg_return_pct,
        stdev(candle_return_pct)                        as volatility,
        avg(price_range)                                as avg_price_range,
        avg(volume)                                     as avg_volume,
        avg(buy_pressure_ratio)                         as avg_buy_pressure,
        sum(case when is_bullish = 1
            then 1 else 0 end) * 100.0
            / count(*)                                  as bullish_pct
    from base
    group by hour, trading_session
)

select * from hourly
