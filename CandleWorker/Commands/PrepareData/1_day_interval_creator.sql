--Построение временных интервалов для анализа
--Интервалы

    --1 день


drop table if exists public.tmp_union_history_candles_all_scales;


--Создаем таблицу для слепка с оригинальными данными по всем интервалам и расчитываем ранги для часов, дней, недель, месяцев
create table if not exists public.tmp_union_history_candles_all_scales
(
    scale             varchar,
    figi              varchar,
    candle_start_dt_utc   timestamp,
    open_price        real,
    close_price       real,
    max_price         real,
    min_price         real,
    volume            integer,
    source_filename   varchar,
    data_source       varchar,
    insertdate_msk    timestamp,
    guidfromfile      varchar,
    rank_by_hour    integer,
    rank_by_day     integer,
    rank_by_week    integer,
    rank_by_month   integer
);

alter table public.tmp_union_history_candles_all_scales
    owner to postgres;

insert into tmp_union_history_candles_all_scales
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
    insertdate_msk,
    guidfromfile,
    rank_by_hour,
    rank_by_day,
    rank_by_week,
    rank_by_month
)
    select
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
    insertdate_msk,
    guidfromfile,
    ROW_NUMBER() OVER(PARTITION BY date_trunc('hour', candle_start_dt_utc) ORDER BY candle_start_dt_utc) as rank_by_hour,
    ROW_NUMBER() OVER(PARTITION BY date_trunc('day', candle_start_dt_utc) ORDER BY candle_start_dt_utc) as rank_by_day,
    ROW_NUMBER() OVER(PARTITION BY date_trunc('week', candle_start_dt_utc) ORDER BY candle_start_dt_utc) as rank_by_week,
    ROW_NUMBER() OVER(PARTITION BY date_trunc('month', candle_start_dt_utc) ORDER BY candle_start_dt_utc) as rank_by_month
    from public.union_history_candles;

-------
--Интервал 1 день--
--Создаем временную таблицу для расчета часового интеревала
--формируем временную таблицу.
-- Необходимо из-за дополнительного шага для расчета цены открытия и закрытия.
create table if not exists public.tmp_1day_union_history_candles_all_scales
(
    scale             varchar,
    figi              varchar,
    candle_start_dt_utc   timestamp,
    open_price        real,
    close_price       real,
    max_price         real,
    min_price         real,
    volume            integer,
    type_of_candle    char,
    is_closed_candle   boolean,
    source_filename   varchar,
    data_source       varchar,
    insertdate_utc   timestamp,
    guidfromfile      varchar
);
comment on column public.tmp_1day_union_history_candles_all_scales.type_of_candle is 'Свеча восходящая (U) / Свеча нисходящая (D) / Свеча нейтральная (N)';
alter table public.union_history_candles_all_scales
    owner to postgres;


--Заполняем временную таблицу данными по часовым интервалам и минимальной+максимальной ценам
-- цены открытия и закрытия будут расчитаны в следующем шаге скрипта
INSERT INTO public.tmp_1day_union_history_candles_all_scales
(
    scale,
    figi,
    candle_start_dt_utc,
    max_price,
    min_price,
    volume,
    source_filename,
    data_source,
    guidfromfile,
    is_closed_candle --признак закрытия свечи
    ,insertdate_utc
)
with summary as
(
    SELECT
        '1_day_scale' as scale,
        figi,
        date_trunc('day',public.union_history_candles.candle_start_dt_utc) as candle_start_dt_utc,
        max(public.union_history_candles.max_price) as max_price,
        min(public.union_history_candles.min_price) as min_price,
        sum(public.union_history_candles.volume) as volume,
        'script_for_1_day_intervals_intervals' as source_filename,
        public.union_history_candles.data_source,
        public.union_history_candles.guidfromfile
    FROM public.union_history_candles
    group by
        figi
        ,date_trunc('day',public.union_history_candles.candle_start_dt_utc)
        ,public.union_history_candles.guidfromfile
        ,public.union_history_candles.data_source
)
select
    summary.scale
    ,summary.figi
    ,summary.candle_start_dt_utc
    ,summary.max_price
    ,summary.min_price
    ,summary.volume
    ,summary.source_filename
    ,summary.data_source
    ,summary.guidfromfile
    ,case
        when summary.candle_start_dt_utc
            between date_trunc('day', now()::timestamp)
                and date_trunc('day',now()::timestamp + interval '1 day')
                    then  false --если время свечи попадает в текущий интервал считаем что свеча не закрыта
        else true --если время не в интервале - считаем, что свеча закрыта
    end as is_closed_candle
,(current_timestamp at time zone 'UTC')
from summary;

--Создаем временную таблицу для хранения цен открытия и закрытия дневных свечей
drop table if exists public.tmp_day_candle_opcl_prices;
CREATE TABLE public.tmp_day_candle_opcl_prices
(
    figi varchar,
    candle_dt timestamp,
    open_price real,
    close_price real,
    rank_by_hour_figi int
);
alter table public.tmp_day_candle_opcl_prices
    owner to postgres;

--Заполняем таблицу для хранения цен открытия и закрытия часовых свечей
insert into public.tmp_day_candle_opcl_prices
select
     openPrice_table.figi
     ,openPrice_table.candle_start
     ,openPrice_table.open_price
     ,closePrice_table_sub.close_price
    ,ROW_NUMBER() OVER(PARTITION BY openPrice_table.figi ORDER BY openPrice_table.candle_start)
 from ((
WITH summary AS
(
select --выбираем минимальный ранг, расчитанный ранее по дневному интервалу
    tuhcas.figi --идентификатор инструмента
    ,date_trunc('day', tuhcas.candle_start_dt_utc) as candle_start --вычисляем нужный интервал тут часовой
    ,min(tuhcas.rank_by_day) rank_min -- минимальное значения ранга в интервале - таким образом находим самое первую запись в цепочке
from public.tmp_union_history_candles_all_scales tuhcas
group by tuhcas.figi, candle_start
) --выбираем минимальный ранг, расчитанный ранее по 15-ти минутным интервалам
select --определяем цену открытия свечи для интервала
    tuhcas.figi --идентификатор инструмента
    ,summary.candle_start --часовой интервал
    ,tuhcas.open_price --цена открытия свечи
from summary
    left join tmp_union_history_candles_all_scales tuhcas
on          summary.figi = tuhcas.figi --figi должны соответсвовать
       and  summary.candle_start
                = date_trunc('day',tuhcas.candle_start_dt_utc) --ищем аналогичный интервал в исходных данных
       and  summary.rank_min = tuhcas.rank_by_day --ищем начало интервала по рангу (ранг используем как идентификатор)
) as openPrice_table
left join --опредеяем цену закрытия
     (
select
    closePrice_table.figi
    ,closePrice_table.candle_start
    ,closePrice_table.close_price
from (
WITH summary AS
(
    select
        tuhcas.figi --идентификатор инструмента
        ,date_trunc('day', tuhcas.candle_start_dt_utc) as candle_start --дневной
        ,max(tuhcas.rank_by_day) rank -- максимальное значения ранга в интервале - находим максимальное значение в цепочке
    from tmp_union_history_candles_all_scales tuhcas
    group by tuhcas.figi, candle_start
)
select
    tuhcas.figi
    ,summary.candle_start
    ,close_price
from summary
    left join tmp_union_history_candles_all_scales tuhcas
on          summary.figi = tuhcas.figi
       and  summary.candle_start = date_trunc('day', tuhcas.candle_start_dt_utc) --ищем аналогичный интервал в исходных данных
       and  summary.rank = tuhcas.rank_by_day ) as closePrice_table) as closePrice_table_sub
on openPrice_table.figi = closePrice_table_sub.figi
and openPrice_table.candle_start = closePrice_table_sub.candle_start);

--Обогащем таблицу с свечами данными цен открытия и закрытия свечей
update tmp_1day_union_history_candles_all_scales t1duhcas
SET
    open_price = thcop.open_price
    ,close_price = thcop.close_price
from public.tmp_day_candle_opcl_prices thcop
where  t1duhcas.figi = thcop.figi
    and t1duhcas.candle_start_dt_utc = thcop.candle_dt;


drop table if exists public.tmp_day_candle_opcl_prices;


--Учти что при формировании часовых интервалов в расчет принимаеются все свечи, в том числе из текущего часа/дня/недели/месяца.
    --Нужно добавить к итоговой таблице признак незакрытой свечи и иметь возможность пересчитыва и перезаписывать такие свечи.

--Тут нужно вставить код расчета свечей по интервалам
    --день
    --неделя
    --месяц



--Переносим информацию по закрытым свечам в хранилище
--вставляем в целевую таблицу расчитанные свечи, в том числе незакрытые свечи.
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
 type_of_candle,
 is_closed_candle,
 source_filename,
 data_source,
 insertdate_utc,
 guidfromfile
)
select
    t1duhcas.scale,
    t1duhcas.figi,
    t1duhcas.candle_start_dt_utc,
    t1duhcas.open_price,
    t1duhcas.close_price,
    t1duhcas.max_price,
    t1duhcas.min_price,
    t1duhcas.volume,
    t1duhcas.type_of_candle,
    t1duhcas.is_closed_candle,
    t1duhcas.source_filename,
    t1duhcas.data_source,
    t1duhcas.insertdate_utc,
    t1duhcas.guidfromfile
from public.tmp_1day_union_history_candles_all_scales t1duhcas
left join public.union_history_candles_all_scales uhcas
    on t1duhcas.figi = uhcas.figi
    and t1duhcas.candle_start_dt_utc = uhcas.candle_start_dt_utc
    and t1duhcas.scale = uhcas.scale
where uhcas.figi is null; --вставляем только отсутсвующие свечи

--обновляем информацию по незакрытым свечам в целевой таблице с масштабами
update public.union_history_candles_all_scales uhcas
set
    max_price = t1duhcas.max_price,
    min_price = t1duhcas.min_price,
    close_price = t1duhcas.close_price,
    is_closed_candle = t1duhcas.is_closed_candle
from tmp_1day_union_history_candles_all_scales t1duhcas
where
    uhcas.figi = t1duhcas.figi
    and uhcas.candle_start_dt_utc = t1duhcas.candle_start_dt_utc
    and uhcas.is_closed_candle = false --обновляем только незакрытые свечи
    and uhcas.scale = t1duhcas.scale;

--удаляем использованную таблицу с данными по часовым интервалам
drop table if exists public.tmp_1day_union_history_candles_all_scales;

--очистка временной таблицы после завершения
drop table if exists public.tmp_union_history_candles_all_scales;