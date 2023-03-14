--расчитываем максимальные значения

--создаем времменую таблицу для прихранивания расчетов
drop table if exists public.tmp_union_candles_all_intervals;
create table if not exists public.tmp_union_candles_all_intervals
(
    candle_id      integer,
    calculate_type varchar,
    interval_3     real,
    interval_5     real,
    interval_10    real,
    interval_12    real,
    interval_15    real,
    interval_16    real,
    interval_20    real,
    interval_26    real,
    interval_50    real,
    interval_60    real,
    interval_90    real,
    interval_100   real,
    interval_120   real,
    interval_150   real,
    interval_200   real,
    insert_dt timestamp,
    update_dt timestamp
);

alter table public.tmp_union_candles_all_intervals
    owner to postgres;

---производим вычисления 'MAXIMUM' и сохраняем в таблицу tmp_union_candles_all_intervals
with interval_3 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 3 preceding and 0 following
             )
     order by candle_start_dt_utc desc)
   ,interval_5 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 5 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_10 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 10 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_12 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 12 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_15 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 15 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_16 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 16 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_20 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 20 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_26 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 26 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_50 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 50 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_60 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 60 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_90 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 90 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_100 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 100 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_120 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 120 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_150 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 150 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_200 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , max(max_price) over w as maximum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 200 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   , sub_query as (
    select
        i3.id
         ,'MAXIMUM' as calculate_type
         ,i3.maximum as interval_3
         ,i5.maximum as interval_5
         ,i10.maximum as interval_10
         ,i12.maximum as interval_12
         ,i15.maximum as interval_15
         ,i16.maximum as interval_16
         ,i20.maximum as interval_20
         ,i26.maximum as interval_26
         ,i50.maximum as interval_50
         ,i60.maximum as interval_60
         ,i90.maximum as interval_90
         ,i100.maximum as interval_100
         ,i120.maximum as interval_120
         ,i150.maximum as interval_150
         ,i200.maximum as interval_200
         ,now()::timestamp as insert_dt
         ,now()::timestamp  as update_dt
    from interval_3 i3
             left join interval_5 i5
                       on i3.id  = i5.id
             left join interval_10 i10
                       on i3.id = i10.id
             left join interval_12 i12
                       on i3.id = i12.id
             left join interval_15 i15
                       on i3.id = i15.id
             left join interval_16 i16
                       on i3.id = i16.id
             left join interval_20 i20
                       on i3.id = i20.id
             left join interval_26 i26
                       on i3.id = i26.id
             left join interval_50 i50
                       on i3.id = i50.id
             left join interval_60 i60
                       on i3.id = i60.id
             left join interval_90 i90
                       on i3.id = i90.id
             left join interval_100 i100
                       on i3.id = i100.id
             left join interval_120 i120
                       on i3.id = i120.id
             left join interval_150 i150
                       on i3.id = i150.id
             left join interval_200 i200
                       on i3.id = i200.id
    order by i3.candle_start_dt_utc desc)
insert into tmp_union_candles_all_intervals
select sq.* from sub_query sq;

---производим вычисления 'MINIMUM' и сохраняем в таблицу tmp_union_candles_all_intervals
with interval_3 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 3 preceding and 0 following
             )
     order by candle_start_dt_utc desc)
   ,interval_5 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 5 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_10 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 10 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_12 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 12 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_15 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 15 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_16 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 16 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_20 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 20 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_26 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 26 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_50 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 50 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_60 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 60 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_90 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 90 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_100 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 100 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_120 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 120 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_150 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 150 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_200 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , min(max_price) over w as minimum
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 200 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   , sub_query as (
    select
        i3.id
         ,'MINIMUM' as calculate_type
         ,i3.minimum as interval_3
         ,i5.minimum as interval_5
         ,i10.minimum as interval_10
         ,i12.minimum as interval_12
         ,i15.minimum as interval_15
         ,i16.minimum as interval_16
         ,i20.minimum as interval_20
         ,i26.minimum as interval_26
         ,i50.minimum as interval_50
         ,i60.minimum as interval_60
         ,i90.minimum as interval_90
         ,i100.minimum as interval_100
         ,i120.minimum as interval_120
         ,i150.minimum as interval_150
         ,i200.minimum as interval_200
         ,now()::timestamp as insert_dt
         ,now()::timestamp as update_dt
    from interval_3 i3
             left join interval_5 i5
                       on i3.id  = i5.id
             left join interval_10 i10
                       on i3.id = i10.id
             left join interval_12 i12
                       on i3.id = i12.id
             left join interval_15 i15
                       on i3.id = i15.id
             left join interval_16 i16
                       on i3.id = i16.id
             left join interval_20 i20
                       on i3.id = i20.id
             left join interval_26 i26
                       on i3.id = i26.id
             left join interval_50 i50
                       on i3.id = i50.id
             left join interval_60 i60
                       on i3.id = i60.id
             left join interval_90 i90
                       on i3.id = i90.id
             left join interval_100 i100
                       on i3.id = i100.id
             left join interval_120 i120
                       on i3.id = i120.id
             left join interval_150 i150
                       on i3.id = i150.id
             left join interval_200 i200
                       on i3.id = i200.id
    order by i3.candle_start_dt_utc desc)
insert into tmp_union_candles_all_intervals
select sq.* from sub_query sq;

---производим вычисления 'MOVING_AVG_CLOSE' и сохраняем в таблицу tmp_union_candles_all_intervals
with interval_3 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 3 preceding and 0 following
             )
     order by candle_start_dt_utc desc)
   ,interval_5 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 5 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_10 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 10 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_12 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 12 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_15 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 15 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_16 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 16 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_20 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 20 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_26 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 26 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_50 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 50 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_60 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 60 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_90 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 90 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_100 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 100 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_120 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 120 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_150 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 150 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   ,interval_200 as
    (select id
          , figi
          , candle_start_dt_utc
          , scale
          , avg(close_price) over w as MOVING_AVG
     from public.union_history_candles_all_scales
     where EXTRACT(DOW from candle_start_dt_utc) not in (0,6)
       and candle_start_dt_utc > now()::timestamp - interval '3 day'
         window w as
             (
             partition by figi, scale
             order by candle_start_dt_utc
             rows between 200 preceding and 0 following
             )
     order by candle_start_dt_utc desc

    )
   , sub_query as (
    select
        i3.id
         ,'MOVING_AVG_CLOSE' as calculate_type
         ,i3.MOVING_AVG as interval_3
         ,i5.MOVING_AVG as interval_5
         ,i10.MOVING_AVG as interval_10
         ,i12.MOVING_AVG as interval_12
         ,i15.MOVING_AVG as interval_15
         ,i16.MOVING_AVG as interval_16
         ,i20.MOVING_AVG as interval_20
         ,i26.MOVING_AVG as interval_26
         ,i50.MOVING_AVG as interval_50
         ,i60.MOVING_AVG as interval_60
         ,i90.MOVING_AVG as interval_90
         ,i100.MOVING_AVG as interval_100
         ,i120.MOVING_AVG as interval_120
         ,i150.MOVING_AVG as interval_150
         ,i200.MOVING_AVG as interval_200
         ,now()::timestamp as insert_dt
         ,now()::timestamp as update_dt
    from interval_3 i3
             left join interval_5 i5
                       on i3.id  = i5.id
             left join interval_10 i10
                       on i3.id = i10.id
             left join interval_12 i12
                       on i3.id = i12.id
             left join interval_15 i15
                       on i3.id = i15.id
             left join interval_16 i16
                       on i3.id = i16.id
             left join interval_20 i20
                       on i3.id = i20.id
             left join interval_26 i26
                       on i3.id = i26.id
             left join interval_50 i50
                       on i3.id = i50.id
             left join interval_60 i60
                       on i3.id = i60.id
             left join interval_90 i90
                       on i3.id = i90.id
             left join interval_100 i100
                       on i3.id = i100.id
             left join interval_120 i120
                       on i3.id = i120.id
             left join interval_150 i150
                       on i3.id = i150.id
             left join interval_200 i200
                       on i3.id = i200.id
    order by i3.candle_start_dt_utc desc)
insert into tmp_union_candles_all_intervals
select sq.* from sub_query sq;

---Вставляем новые записи в целевую таблицу с вычислениями

insert into public.union_candles_all_intervals
select tucai.* from tmp_union_candles_all_intervals tucai
                        left join public.union_candles_all_intervals ucai
                                  on tucai.candle_id = ucai.candle_id
where ucai.candle_id is null;


---Обновляем существующие записи в таблице

update public.union_candles_all_intervals ucai
set
    update_dt = now()::timestamp
  ,interval_3 = tucai.interval_3
  ,interval_5 = tucai.interval_5
  ,interval_10 = tucai.interval_10
  ,interval_12 = tucai.interval_12
  ,interval_15 = tucai.interval_15
  ,interval_16 = tucai.interval_16
  ,interval_20 = tucai.interval_20
  ,interval_26 = tucai.interval_26
  ,interval_50 = tucai.interval_50
  ,interval_60 = tucai.interval_60
  ,interval_90 = tucai.interval_90
  ,interval_100 = tucai.interval_100
  ,interval_120 = tucai.interval_120
  ,interval_150 = tucai.interval_150
  ,interval_200 = tucai.interval_200
from tmp_union_candles_all_intervals tucai
where
        tucai.candle_id = ucai.candle_id
  and tucai.calculate_type = ucai.calculate_type
  and (
            tucai.interval_3 <> ucai.interval_3
        or tucai.interval_5 <> ucai.interval_5
        or tucai.interval_10 <> ucai.interval_10
        or tucai.interval_12 <> ucai.interval_12
        or tucai.interval_15 <> ucai.interval_15
        or tucai.interval_16 <> ucai.interval_16
        or tucai.interval_20 <> ucai.interval_20
        or tucai.interval_26 <> ucai.interval_26
        or tucai.interval_50 <> ucai.interval_50
        or tucai.interval_60 <> ucai.interval_60
        or tucai.interval_90 <> ucai.interval_90
        or tucai.interval_100 <> ucai.interval_100
        or tucai.interval_120 <> ucai.interval_120
        or tucai.interval_150 <> ucai.interval_150
        or tucai.interval_200 <> ucai.interval_200);


--Удаляем временную таблицу с расчетами
drop table if exists public.tmp_union_candles_all_intervals;