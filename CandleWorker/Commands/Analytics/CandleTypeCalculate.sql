--Вычисляем тип свечей (восходящая или нисходящая) относительно предыдущего закрытия


--Вычисление типа свечи производится для
--  свечей с атрибутом is_closed = true
--  свечей с атрибутом type_of_canldes is null


--Создаем таблицу - формирования выборки для расчета типа свечи
drop table if exists public.tmp_candles_for_candle_type_calculate;
create table if not exists public.tmp_candles_for_candle_type_calculate
(
    candle_id               int
    ,scale                  varchar
    ,figi                   varchar
    ,candle_start_dt_utc    timestamp
    ,close_price            real
    ,rank_number_by_figi_scale int
);
alter table public.tmp_candles_for_candle_type_calculate
    owner to postgres;
delete from public.tmp_candles_for_candle_type_calculate where 1=1;

--формируем выборку и записываем ее в ранее созданную таблицу
insert into public.tmp_candles_for_candle_type_calculate
(candle_id, scale, figi, candle_start_dt_utc, close_price, rank_number_by_figi_scale)
select
    uhcas.id,
    uhcas.scale,
    uhcas.figi,
    uhcas.candle_start_dt_utc,
    uhcas.close_price,
    ROW_NUMBER()
        OVER(PARTITION BY uhcas.figi, uhcas.scale
            ORDER BY uhcas.candle_start_dt_utc) as rank_number_by_figi_scale

from public.union_history_candles_all_scales uhcas
where candle_start_dt_utc >
    (--если выбирать свечи без этого условия, то придется каждый раз формировать полную выборку.
        --если же оганичить по минимальной дате -(минус) 1 день от той свечи, которая является самой старой нерасчитанной свечей
        -- - мы сможем серьезно ограничить выборку и не нагружать сервер.
    select date_trunc('day',min(candle_start_dt_utc)::timestamp - interval '1 day')
    from union_history_candles_all_scales
    where type_of_candle is null
    )
order by id;


--создаем таблицу для расчета типа свечи
--Таблица содержит поля для текущей и предыдущей свечи.
drop table if exists public.tmp_canlde_type_calculates;
create table if not exists public.tmp_canlde_type_calculates
(
id_current_candle           int
,id_prev_candle                 int
,scale                          varchar
,figi                           varchar
,prev_candle_close_price        real
,current_candle_close_price     real
,type_of_candle                 char
,different_between_close_prices real
);
alter table public.tmp_canlde_type_calculates
    owner to postgres;
delete from public.tmp_canlde_type_calculates where 1=1;

--Записываем информацию о типе свечей и разнице в цене между ними в помежуточную таблицу
insert into public.tmp_canlde_type_calculates
select
    t1.candle_id as current_candle
    ,t2.candle_id as prev_candle
    ,t1.scale
    ,t1.figi
    ,t2.close_price prev_candle_close_price
    ,t1.close_price current_candle_close_price
,CASE
    WHEN t2.close_price < t1.close_price THEN 'U' --UpCandle
    WHEN t2.close_price > t1.close_price THEN 'D' -- DownCandle
    WHEN t2.candle_id is null THEN NULL
    else 'N' --Neutral Candle
END dayType
,CASE
    WHEN t2.close_price < t1.close_price THEN t1.close_price - t2.close_price --Если текущая свеча закрылась ВЫШЕ предыдущей свечи
                                                                                    -- - вычисляем разницу между ПРЕДЫДУЩЕЙ и ТЕКУЩЕЙ свечами
    WHEN t2.close_price > t1.close_price THEN t2.close_price - t1.close_price --Если текущая свеча закрылась НИЖЕ предыдущей свечи
                                                                                    -- - вычисляем разницу между ТЕКУЩЕЙ и ПРЕДЫДУЩЕЙ свечами
    WHEN t2.candle_id is null THEN NULL
    else 0 --Neutral Candle
END different_between_close_prices
from public.tmp_candles_for_candle_type_calculate t1 -- t1 - CurrentCandle
left join
    public.tmp_candles_for_candle_type_calculate t2 on  -- t2 - PrevCandle
        t1.figi = t2.figi and t1.scale=t2.scale
        and t1.rank_number_by_figi_scale = (t2.rank_number_by_figi_scale + 1) -- условие rank = (rank + 1) читается как: сцепи текущую (rank) и предыдущую (rank+1)
        -- ДА-ДА-ДА, именно +1 а не минус.
        -- Фактически логика работает обратным образом: мы ищем rank, который на единицу меньше и прибавляем к нему единицу, чтобы сцепить с текущим.
order by t1.candle_id;

--В таблицу с масштабами union_history_candles_all_scales вносим информацию о типе свечи (восходящая или нисходящая)
update public.union_history_candles_all_scales
set
    type_of_candle = t1.type_of_candle
from tmp_canlde_type_calculates t1
where t1.id_current_candle = union_history_candles_all_scales.id;

--удаляем временные таблицы
drop table public.tmp_candles_for_candle_type_calculate;
drop table public.tmp_canlde_type_calculates;



