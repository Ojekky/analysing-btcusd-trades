with source as (
    select * from {{ source('gold_btc_data', 'btcusdt_1h_klines') }}
),

renamed as (
    select
        open_time                                   as candle_time,
        datepart(year,  open_time)                  as [year],
        datepart(month, open_time)                  as [month],
        datepart(day,   open_time)                  as [day],
        datepart(hour,  open_time)                  as [hour],
        cast([open]  as decimal(18,8))              as [open],
        cast(high    as decimal(18,8))              as high,
        cast(low     as decimal(18,8))              as low,
        cast([close] as decimal(18,8))              as [close],
        cast(volume       as decimal(24,8))         as volume,
        cast(quote_volume as decimal(24,8))         as quote_volume,
        cast([count]      as bigint)                as trade_count,
        cast(taker_buy_base  as decimal(24,8))      as taker_buy_base,
        cast(taker_buy_quote as decimal(24,8))      as taker_buy_quote,
        close_time
    from source
)

select * from renamed
