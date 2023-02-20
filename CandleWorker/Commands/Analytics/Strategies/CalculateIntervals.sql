--Формируем вспомогательную таблицу с нужными интервалами.
--Будет применяться при вычислении интервальных метрик типа EMA, SMA и их производных

--Создаем временную таблицу для предварительно расчитанных интервалов
drop table if exists public.tmp_union_candles_intervals_all_scales;
create table if not exists public.tmp_union_candles_intervals_all_scales
(
    parrent_candle_id                  integer,
    parrent_candle_figi                varchar,
    parrent_candle_start_dt_utc        timestamp,
    start_interval_row_id              integer,
    start_interval_candle_start_dt_utc timestamp,
    prev_5_row_id                      integer,
    prev_5_candle_start_dt_utc         timestamp,
    prev_10_row_id                     integer,
    prev_10_candle_start_dt_utc        timestamp,
    prev_12_row_id                     integer,
    prev_12_candle_start_dt_utc        timestamp,
    prev_16_row_id                     integer,
    prev_16_candle_start_dt_utc        timestamp,
    prev_20_row_id                     integer,
    prev_20_candle_start_dt_utc        timestamp,
    prev_26_row_id                     integer,
    prev_26_candle_start_dt_utc        timestamp,
    prev_50_row_id                     integer,
    prev_50_candle_start_dt_utc        timestamp,
    prev_60_row_id                     integer,
    prev_60_candle_start_dt_utc        timestamp,
    prev_100_row_id                    integer,
    prev_100_candle_start_dt_utc       timestamp,
    prev_120_row_id                    integer,
    prev_120_candle_start_dt_utc       timestamp,
    prev_200_row_id                    integer,
    prev_200_candle_start_dt_utc       timestamp
);


--Расчитываем необходимые интервалы за последние 7 календарных дней
--  (при первом проходе ограничение 7 дней должно быть убрано вручную в скрипте)
with sub_query_donchian_intervals as
    (
    with sub_query as
    (
        select
        ROW_NUMBER() OVER(PARTITION BY figi,scale ORDER BY candle_start_dt_utc) as rank_by_scale
        ,*
        from public.union_history_candles_all_scales uhcas
        where EXTRACT(DOW FROM uhcas.candle_start_dt_utc)
                  not in (6,7)  ---Не учитываем движения цены в выходные дни. Поскольку из-за низкой ликвидности на рынке, возможны аномальные движения цен
            --and uhcas.candle_start_dt_utc > now() - interval '7 day'
        )
select
      sq.id as current_candle_id
     ,sq.figi
     ,sq.candle_start_dt_utc as current_candle_start_dt_utc

     ,t1.id as start_interval_row_id
     ,t1.candle_start_dt_utc as start_interval_candle_start_dt_utc

     ,t5.id as prev_5_row_id
     ,t5.candle_start_dt_utc as prev_5_candle_start_dt_utc

     ,t10.id as prev_10_row_id
     ,t10.candle_start_dt_utc as prev_10_candle_start_dt_utc

     ,t12.id as prev_12_row_id
     ,t12.candle_start_dt_utc as prev_12_candle_start_dt_utc

     ,t16.id as prev_16_row_id
     ,t16.candle_start_dt_utc as prev_16_candle_start_dt_utc

     ,t20.id as prev_20_row_id
     ,t20.candle_start_dt_utc as prev_20_candle_start_dt_utc

     ,t26.id as prev_26_row_id
     ,t26.candle_start_dt_utc as prev_26_candle_start_dt_utc

     ,t50.id as prev_50_row_id
     ,t50.candle_start_dt_utc as prev_50_candle_start_dt_utc

     ,t60.id as prev_60_row_id
     ,t60.candle_start_dt_utc as prev_60_candle_start_dt_utc

     ,t100.id as prev_100_row_id
     ,t100.candle_start_dt_utc as prev_100_candle_start_dt_utc

     ,t120.id as prev_120_row_id
     ,t120.candle_start_dt_utc as prev_120_candle_start_dt_utc

     ,t200.id as prev_200_row_id
     ,t200.candle_start_dt_utc as prev_200_candle_start_dt_utc
from sub_query sq
left join sub_query t5 on sq.rank_by_scale = t5.rank_by_scale + 5
    and sq.scale = t5.scale
    and sq.figi = t5.figi
left join sub_query t10 on sq.rank_by_scale = t10.rank_by_scale + 10
    and sq.scale = t10.scale
        and sq.figi = t10.figi
left join sub_query t12 on sq.rank_by_scale = t12.rank_by_scale + 12
    and sq.scale = t12.scale
    and sq.figi = t12.figi
left join sub_query t16 on sq.rank_by_scale = t16.rank_by_scale + 16
    and sq.scale = t16.scale
    and sq.figi = t16.figi
left join sub_query t20 on sq.rank_by_scale = t20.rank_by_scale + 20
    and sq.scale = t20.scale
    and sq.figi = t20.figi
left join sub_query t26 on sq.rank_by_scale = t26.rank_by_scale + 26
    and sq.scale = t26.scale
    and sq.figi = t26.figi
left join sub_query t50 on sq.rank_by_scale = t50.rank_by_scale + 50
    and sq.scale = t50.scale
    and sq.figi = t50.figi
left join sub_query t60 on sq.rank_by_scale = t60.rank_by_scale + 60
    and sq.scale = t60.scale
    and sq.figi = t60.figi
left join sub_query t100 on sq.rank_by_scale = t100.rank_by_scale + 100
    and sq.scale = t100.scale
    and sq.figi = t100.figi
left join sub_query t120 on sq.rank_by_scale = t120.rank_by_scale + 120
    and sq.scale = t120.scale
    and sq.figi = t120.figi
left join sub_query t150 on sq.rank_by_scale = t150.rank_by_scale + 150
    and sq.scale = t150.scale
    and sq.figi = t150.figi
left join sub_query t200 on sq.rank_by_scale = t200.rank_by_scale + 200
    and sq.scale = t200.scale
    and sq.figi = t200.figi
left join sub_query t1 on sq.rank_by_scale = t1.rank_by_scale + 1 --указывает на начало интевала, будет использоваться для вычисления максимальных и минимальных значений
    and sq.scale = t1.scale
    and sq.figi = t1.figi)
--Вставляем расчитанные интервалы в временную таблицу с интервалами
insert into tmp_union_candles_intervals_all_scales
    (parrent_candle_id,
     parrent_candle_figi,
     parrent_candle_start_dt_utc,
     start_interval_row_id,
     start_interval_candle_start_dt_utc,
    prev_5_row_id, prev_5_candle_start_dt_utc, prev_10_row_id,
    prev_10_candle_start_dt_utc, prev_12_row_id,
    prev_12_candle_start_dt_utc, prev_16_row_id,
    prev_16_candle_start_dt_utc, prev_20_row_id,
    prev_20_candle_start_dt_utc, prev_26_row_id,
    prev_26_candle_start_dt_utc, prev_50_row_id,
    prev_50_candle_start_dt_utc, prev_60_row_id,
    prev_60_candle_start_dt_utc, prev_100_row_id,
    prev_100_candle_start_dt_utc, prev_120_row_id,
    prev_120_candle_start_dt_utc, prev_200_row_id,
    prev_200_candle_start_dt_utc)
select
    sqdi.*
from sub_query_donchian_intervals sqdi;

--Переносим недостающие строки в конечную таблицу с интервалами
--  Вставляем только отсутсвующие (новые строки)
insert into union_candles_intervals_all_scales
select
    tucias.*
from tmp_union_candles_intervals_all_scales tucias
left join public.union_candles_intervals_all_scales ucias
    on ucias.parrent_candle_id = tucias.parrent_candle_id
where ucias.parrent_candle_id is null;---вставляем только те строки, которых нет в конечной таблице

--Обновляем значения в существующих строках
update union_candles_intervals_all_scales ucias
set
    prev_5_row_id = tucias.prev_5_row_id
    ,prev_10_row_id = tucias.prev_10_row_id
    ,prev_12_row_id = tucias.prev_12_row_id
    ,prev_16_row_id = tucias.prev_16_row_id
    ,prev_20_row_id = tucias.prev_20_row_id
    ,prev_26_row_id = tucias.prev_26_row_id
    ,prev_50_row_id = tucias.prev_50_row_id
    ,prev_60_row_id = tucias.prev_60_row_id
    ,prev_100_row_id = tucias.prev_100_row_id
    ,prev_120_row_id = tucias.prev_120_row_id
    ,prev_200_row_id = tucias.prev_200_row_id
from tmp_union_candles_intervals_all_scales tucias
where ucias.parrent_candle_id = tucias.parrent_candle_id;

--Удаляем временную таблицу с интервалами
drop table if exists public.tmp_union_candles_intervals_all_scales;