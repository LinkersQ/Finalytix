
                   (with summary_main as
                             (with summary as
                                       (select id,
                                               scale,
                                               candle_start_dt_utc,
                                               figi,
                                               type_of_candle,
                                               close_price,
                                               ROW_NUMBER()
                                               OVER (PARTITION BY uhcas.figi, uhcas.scale
                                                   ORDER BY uhcas.candle_start_dt_utc) as rank_number_by_figi_scale
                                        from union_history_candles_all_scales uhcas
                                        where scale = '1_day_scale'
                                        order by candle_start_dt_utc)
                              select summary.id
                                   , summary.scale
                                   , summary.candle_start_dt_utc
                                   , summary.figi
                                   , summary.close_price                           as current_close_price
                                   , summary_2.close_price                         as prev_close_price
                                   , (summary.close_price - summary_2.close_price) as different_current_prev
                              from summary
                                       left join
                                   (select id,
                                           scale,
                                           candle_start_dt_utc,
                                           figi,
                                           type_of_candle,
                                           close_price,
                                           ROW_NUMBER()
                                           OVER (PARTITION BY uhcas.figi, uhcas.scale
                                               ORDER BY uhcas.candle_start_dt_utc) as rank_number_by_figi_scale
                                    from union_history_candles_all_scales uhcas
                                    where scale = '1_day_scale'
                                    order by candle_start_dt_utc) as summary_2
                                   on summary.rank_number_by_figi_scale = summary_2.rank_number_by_figi_scale + 7
                                       and summary.figi = summary_2.figi)
                    select *
                    from summary_main)
