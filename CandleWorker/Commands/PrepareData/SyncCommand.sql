--Создаем таблицу для временного размещения данных для построения 15-ти минутных интервалов из cold_history_data
-- (цель всего скрипта - заполнить именно эту таблицу для последующей передачи данных в целевую витрину с масштабами)
drop table if exists public.tmp_cold_history_candles;
create table public.tmp_cold_history_candles
(
    scale             varchar,
    figi              varchar,
    candle_start_dt   timestamp,
    open_price        real,
    close_price       real,
    max_price         real,
    min_price         real,
    volume            integer,
    source_filename   varchar,
    insertdate        timestamp,
    guidfromfile      varchar,
    rank_by_15min      integer,
    rank_by_15min_figi  integer
);

alter table public.tmp_cold_history_candles
    owner to postgres;

--Временная таблица для хранения оригинальных данных + rank по 15-ти минутам
drop table if exists public.tmp_cold_history_candles_original;
create table public.tmp_cold_history_candles_original
(
    scale             varchar,
    figi              varchar,
    candle_start_dt   timestamp,
    open_price        real,
    close_price       real,
    max_price         real,
    min_price         real,
    volume            integer,
    source_filename   varchar,
    insertdate        timestamp,
    guidfromfile      varchar,
    rank_by_15min      integer,
    rank_by_15min_figi  integer
);

alter table public.tmp_cold_history_candles_original
    owner to postgres;

--вносим в tmp_cold_history_candles_original оригинальные данные + ранг по 15-ти минутным интревалам
insert into public.tmp_cold_history_candles_original
(
 scale
 ,figi
 ,candle_start_dt
 ,open_price
 ,close_price
 ,max_price
 ,min_price
 ,volume
 ,source_filename
 ,insertdate
 ,guidfromfile
 ,rank_by_15min
)
select
    'original'
    ,figi
    ,candle_start_dt
    ,open_price
    ,close_price
    ,max_price
    ,min_price
    ,volume
    ,source_filename
    ,insertdate
    ,guidfromfile
    ,ROW_NUMBER() OVER(PARTITION BY to_timestamp(floor((extract('epoch' from candle_start_dt) / 900 )) * 900)
                                        AT TIME ZONE 'UTC' ORDER BY candle_start_dt asc) --Ранг внутри 15-ти минутных интервалов
                                                                                        --используется для связки в процессе вычисления цен Open и Close
from cold_history_candles hcd
where figi = 'BBG004731489'; -- в ПРОМе нужно использовать витрину cold_history_candles

--вносим в tmp_cold_history_candles 15-ти минутные интервалы вместе с расчетными параметрами
insert into tmp_cold_history_candles
    (
     scale
     ,figi
     ,candle_start_dt
     ,max_price
     ,min_price
     ,volume
     ,source_filename
     ,guidfromfile
    )
select
    '15_minutes'
     ,hcd.figi
     ,to_timestamp(floor((extract('epoch' from hcd.candle_start_dt) / 900 )) * 900)
            AT TIME ZONE 'UTC' as interval_15_minutes --фомируем 15-ти минутный интервал
     ,max(hcd.max_price) --вычисляем максимальное значение за период
     ,min(hcd.min_price) --вычисляем минимальное значение за период
     ,sum(hcd.volume) --вычисляем сумму объемов за период
     ,'cold_data_15minute_generation_script'
     ,hcd.guidfromfile
    from tmp_cold_history_candles_original hcd -- в ПРОМе нужно использовать витрину cold_history_candles
group by hcd.figi,interval_15_minutes,hcd.guidfromfile;


--Создаем таблицу для хранения цен открытия и закрытия 15-ти минутных свечей
drop table if exists public.tmp_15min_cold_candles_open_close_price;
CREATE TABLE public.tmp_15min_cold_candles_open_close_price
(
    figi varchar,
    candle_dt timestamp,
    open_price real,
    close_price real,
    rank_by_hour_figi int
);
alter table public.tmp_15min_cold_candles_open_close_price
    owner to postgres;
-----
--Заполняем таблицу для хранения цен открытия и закрытия 15-ти минутных свечей
insert into public.tmp_15min_cold_candles_open_close_price
select
     openPrice_table.figi
     ,openPrice_table.candle_star_dt_15min
     ,openPrice_table.open_price
     ,closePrice_table_sub.close_price
    ,ROW_NUMBER() OVER(PARTITION BY openPrice_table.figi ORDER BY openPrice_table.candle_star_dt_15min)
 from ((
WITH summary AS
(
select --выбираем минимальный ранг, расчитанный ранее по 15-ти минутным интервалам
    tchco.figi --идентификатор инструмента
    ,to_timestamp(floor((extract('epoch' from tchco.candle_start_dt) / 900 )) * 900)
         AT TIME ZONE 'UTC' candle_star_dt_15min --15-ти минутный интервал
    ,min(tchco.rank_by_15min) rank_15min -- минимальное значения ранга в интервале
from tmp_cold_history_candles_original tchco
where scale = 'original'
group by tchco.figi, candle_star_dt_15min
) --выбираем минимальный ранг, расчитанный ранее по 15-ти минутным интервалам
select --определяем цену открытия свечи для интервала
    tchco.figi --идентификатор инструмента
    ,summary.candle_star_dt_15min --15-ти минутный интервал
    ,tchco.open_price --цена открытия свечи
from summary
    left join tmp_cold_history_candles_original tchco
on          summary.figi = tchco.figi --figi должны соответсвовать
       and  summary.candle_star_dt_15min
                = to_timestamp(floor((extract('epoch' from tchco.candle_start_dt) / 900 )) * 900) AT TIME ZONE 'UTC' --ищем аналогичный интервал в исходных данных
       and  summary.rank_15min = tchco.rank_by_15min --ищем начало интервала по рангу (ранг используем как идентификатор)
) as openPrice_table
left join --опредеяем цену закрытия
     (
select
    figi
    ,candle_star_dt_15min
    ,close_price
from (
WITH summary AS
(
    select
        tchco.figi --идентификатор инструмента
        ,to_timestamp(floor((extract('epoch' from tchco.candle_start_dt) / 900 )) * 900)
            AT TIME ZONE 'UTC' candle_star_dt_15min --15-ти минутный интервал
        ,max(tchco.rank_by_15min) rank -- максимальное значения ранга в интервале
    from tmp_cold_history_candles_original tchco
    where scale = 'original'
    group by tchco.figi, candle_star_dt_15min
)
select
    tchco.figi
    ,summary.candle_star_dt_15min
    ,close_price
from summary
    left join tmp_cold_history_candles_original tchco
on          summary.figi = tchco.figi
       and  summary.candle_star_dt_15min = to_timestamp(floor((extract('epoch' from tchco.candle_start_dt) / 900 )) * 900)
                                               AT TIME ZONE 'UTC' --ищем аналогичный интервал в исходных данных
       and  summary.rank = tchco.rank_by_15min ) as closePrice_table) as closePrice_table_sub
on openPrice_table.figi = closePrice_table_sub.figi
and openPrice_table.candle_star_dt_15min = closePrice_table_sub.candle_star_dt_15min);

--Обогащем таблицу с свечами данными цен открытия и закрытия свечей
update tmp_cold_history_candles
SET
    open_price = t15minccocp.open_price
    ,close_price = t15minccocp.close_price
    ,rank_by_15min = t15minccocp.rank_by_hour_figi
from public.tmp_15min_cold_candles_open_close_price t15minccocp
where  tmp_cold_history_candles.figi = t15minccocp.figi
    and tmp_cold_history_candles.candle_start_dt = t15minccocp.candle_dt
    and tmp_cold_history_candles.scale = '15_minutes';




-- tmp_cold_history_candles - таблица хранит истоиические свечи из холодных источнико (Тинькофф АПИ)
--  по 15-ти минутным интревалам
-- warm_history_candles - таблица хранит исторические свечи, полученные из теплого источника (Тинькоф АПИ GetCandles)

--Синхронизируем теплые и холодные данные по 15-ти минутным интервалам
insert into public.union_history_candles
with union_summary as (
select
               '15_minutes_scale' as scale
               ,'cold_data_source' as data_source
               , tchc.figi
               , tchc.candle_start_dt
               , tchc.open_price
               , tchc.close_price
               , tchc.max_price
               , tchc.min_price
               , tchc.volume
               , tchc.source_filename
               , tchc.insertdate
               , tchc.guidfromfile
from public.tmp_cold_history_candles tchc
left join public.warm_history_candles whc
on tchc.figi = whc.figi and tchc.candle_start_dt = whc.candle_start_dt
where whc.candle_start_dt is null
union all
select
                '15_minutes_scale' as scale
               ,'warm_data_source' as data_source
               , figi
               , candle_start_dt
               , open_price
               , close_price
               , max_price
               , min_price
               , volume
               , source_filename
               , insertdate
               , guidfromfile
from warm_history_candles
where is_close_candle = true)
select
                us.scale
               ,us.data_source
               , us.figi
               , us.candle_start_dt
               , us.open_price
               , us.close_price
               , us.max_price
               , us.min_price
               , us.volume
               , us.source_filename
               , us.insertdate
               , us.guidfromfile
from union_summary us left join union_history_candles uhc on us.figi = uhc.figi and
                                                             us.candle_start_dt = uhc.candle_start_dt_utc
                                                                and us.scale = uhc.scale
where uhc.candle_start_dt_utc is null    --определяем те строки, которых в union_history_candles еще нет и добавляем только их
order by us.candle_start_dt;

drop table tmp_cold_history_candles;
drop table tmp_cold_history_candles_original;
drop table tmp_15min_cold_candles_open_close_price;