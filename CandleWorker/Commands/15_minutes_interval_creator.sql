--Построение временных интервалов для анализа
--Интервалы
    --15 минут

--Интервал 15-минут--

insert into public.union_history_candles_all_scales
(
    scale,
    figi,
    candle_start_dt_utc,
    open_price,
    close_price,
    max_price,
    min_price,
    volume,
    source_filename,
    data_source,
    insertdate_utc,
    guidfromfile,
    is_closed_candle
)
with summary as
    (
        select
            '15_minutes_scale' as scale,
            uhc.figi,
            uhc.candle_start_dt_utc,
            uhc.open_price,
            uhc.close_price,
            uhc.max_price,
            uhc.min_price,
            uhc.volume,
            uhc.source_filename,
            uhc.data_source,
            uhc.insertdate_msk,
            uhc.guidfromfile
        from union_history_candles uhc
            left join public.union_history_candles_all_scales uhcas on
                 uhc.scale = '15_minutes_scale'
                 and uhc.candle_start_dt_utc = uhcas.candle_start_dt_utc
                 and uhc.figi = uhcas.figi
                 where uhcas.figi is null
    )
select
    summary.scale,
    summary.figi,
    summary.candle_start_dt_utc,
    summary.open_price,
    summary. close_price,
    summary.max_price,
    summary.min_price,
    summary.volume,
    summary.source_filename,
    summary.data_source,
    (current_timestamp at time zone 'UTC'),
    summary.guidfromfile,
    true --хардкод параметра для 15-ти минутных интервалов. Сделано из-за гарантированного размещения в union_history_candles только закрытых 15-ти минутных свечей
from summary;



