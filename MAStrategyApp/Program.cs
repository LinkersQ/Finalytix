﻿using FinInvestLibrary.Functions.LocalOperations;
using FinInvestLibrary.Objects;
using FinInvestLibrary.Objects.Trade;
using log4net;
using Newtonsoft.Json;

namespace MAStrategyApp

{
    internal class Program
    {
        private static string FAST_INTERVAL = string.Empty;
        private static string SLOW_INTERVAL = string.Empty;
        private static string STRATEGY_NAME = string.Empty;
        private static string TG_CHANNEL_ID = string.Empty;

        public static readonly ILog log = LogManager.GetLogger(typeof(Program));

        private static int Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();
            log.Info(@"/---------Start---------\");

            log.Info("Конфигурирую приложение");
            string runType, strategyName, scaleName, connectionString, tg_channel_id, country_of_market;
            int exitCode = 9999;
            try
            {
#if DEBUG
                runType = "trade_close_point_trades_for_channel";
                strategyName = "MA_12/26";
                scaleName = "1_day_scale";
                tg_channel_id= "-1001820470601";
                country_of_market = "RU";


#else
                runType = args[0];//"trade_close_point";//args[0];
                strategyName = args[1]; //args[1]; //"MA_12/26";
                scaleName = args[2];//args[2];
                tg_channel_id = args[3];
                country_of_market = args[4];
#endif
                string appPath = Environment.CurrentDirectory;
                string connectionStringPath = appPath + "\\connectionString.txt";
                connectionString = File.ReadAllText(connectionStringPath);

                string fastInterval = strategyName.Split('_')[1].Split('/')[0];
                string slowInterval = strategyName.Split('_')[1].Split('/')[1];

                STRATEGY_NAME = strategyName;
                FAST_INTERVAL = fastInterval;
                SLOW_INTERVAL = slowInterval;
                TG_CHANNEL_ID = tg_channel_id;
            }
            catch (Exception ex)
            {
                log.Error("Неверные входные параметры");
                log.Error("Требуется указать: \r\n\trunType (trade_open_point/trade_close_point)\r\n\tstrategyName (MA_12/26 или MA_50/200 полный список доступных стретегий смотри в документации\r\n\tscaleName (1_day_scale)");
                log.Error("exitCode = " + exitCode);
                log.Error(ex.ToString());
                exitCode = 11;
                return exitCode;
            }


            DateTime currentDateTime = DateTime.UtcNow; //Tinkoff API работает всегда в UTC - придерживаемся тоже UTC;



            log.Info("Тип запуска: " + runType);
            log.Info("Рынок: " + country_of_market);
            log.Info("Стратегия: " + STRATEGY_NAME);
            log.Info("Масштаб: " + scaleName);
            log.Info("Строка подключения: " + connectionString);
            log.Info("Конфигурирование завершено");

            //Получаю список активов, по которым требуется рассчитывать и контролировать сделки
            List<ShareObject> shares = GetActualSharesList(country_of_market, connectionString).Where(w => w.country_of_risk.Equals(country_of_market)).Where(w => w.UnavailableForAnalysys is false).ToList();

            log.Info("Работаю по активам: " + shares.Count());
            string shares2Log = string.Empty;
            foreach (var sha in shares.ToList())
            {
                string v1 = sha.name.ToString();
                string v2 = sha.figi.ToString();
                shares2Log += v1 + "(" + v2 + "),";
            }
            log.Debug(shares2Log);


            //Вызов стратегии

            //Запуск в режиме поиска точек входа
            if (runType.Equals("trade_open_point"))
            {
                try
                {
                    log.Info("Запущен режим поиска новых сделок по стратегии " + STRATEGY_NAME);
                    MA_Strategy_OpenTrade(strategyName, scaleName, connectionString, shares);
                    log.Info("Завершен процесс поиска новых сделок по стратегии " + STRATEGY_NAME);
                }
                catch (Exception ex)
                {
                    log.Error("Не удалось завершить процесс поиска входа в сделку по стратегии " + STRATEGY_NAME);
                    log.Error(ex);
                }
            }
            //Запуск в режиме мониторинга активных сделок для аналитики
            else if (runType.Equals("trade_close_point_for_analysis"))
            {
                try
                {
                    log.Info("Запущен режим мониторинга активных сделок (для аналитики) по стратегии " + STRATEGY_NAME);
                    MA_Strategy_TradeWorker(connectionString, scaleName);

                }
                catch (Exception ex)
                {
                    log.Error("Не удалось завершить процесс поиска выхода из сделки");
                    log.Error(ex);
                }
            }
            //Запуск в режиме мониторинга активных сделок для коммуникации
            else if (runType.Equals("trade_close_point_trades_for_channel"))
            {
                try
                {
                    log.Info("Запущен режим мониторинга активных сделок (для рассылки) по стратегии " + STRATEGY_NAME);
                    MA_Strategy_TradeWorker_TradesForChannel(connectionString, scaleName, shares);

                }
                catch (Exception ex)
                {
                    log.Error("Не удалось завершить процесс поиска выхода из сделки");
                    log.Error(ex);
                }
            }


            log.Info(@"\---------Finish--------/");
            return exitCode;
        }

        ///запрос выполняется к двум таблицам.
        ///1-я - таблица со списком акций - Shares
        ///2-я - таблица со списком акций с известными ошибками, по которым не нужно проводить технический анализ.
        ///Select работает через left join. В случае если поле es.ticker as excluded_ticker не равно NULL - в объекте SharesObj проставляется флаг UnavailableForAnalysys
        private static List<ShareObject> GetActualSharesList(string country_of_market, string connectionString)
        {
            var query_result = new PgExecuter(connectionString, log).ExecuteReader("SELECT s.figi, s.ticker, s.class_code, s.isin, s.lot, s.currency, s.short_enabled_flag, s.name, s.exchange, s.issue_size, s.country_of_risk, s.country_of_risk_name, s.sector, s.issue_size_plan, s.trading_status, s.otc_flag, s.buy_available_flag, s.sell_available_flag, s.div_yield_flag, s.share_type, s.min_price_increment, s.api_trade_available_flag, s.uid, s.real_exchange, s.position_uid, s.for_iis_flag, s.for_qual_investor_flag, s.weekend_flag, s.blocked_tca_flag, es.ticker as excluded_ticker FROM public.Shares s left join excluded_shares es on (es.ticker = s.ticker or es.isin = s.isin or es.figi = s.figi)");
            List<ShareObject> shObjList = new List<ShareObject>();
            foreach (var str in query_result)
            {
                var partsOfRow = str.Split(';');
                ShareObject shObj = new ShareObject();
                shObj.figi = partsOfRow[0];
                shObj.ticker = partsOfRow[1];
                shObj.class_code = partsOfRow[2];
                shObj.isin = partsOfRow[3];
                shObj.lot = Convert.ToInt32(partsOfRow[4]);
                shObj.currency = partsOfRow[5];
                shObj.short_enabled_flag = Convert.ToBoolean(partsOfRow[6]);
                shObj.name = partsOfRow[7];
                shObj.exchange = partsOfRow[8];
                shObj.issue_size = Convert.ToInt64(partsOfRow[9]);
                shObj.country_of_risk = partsOfRow[10];
                shObj.country_of_risk_name = partsOfRow[11];
                shObj.sector = partsOfRow[12];
                shObj.issue_size_plan = Convert.ToInt64(partsOfRow[13]);
                shObj.trading_status = partsOfRow[14];
                shObj.otc_flag = Convert.ToBoolean(partsOfRow[15]);
                shObj.buy_available_flag = Convert.ToBoolean(partsOfRow[16]);
                shObj.sell_available_flag = Convert.ToBoolean(partsOfRow[17]);
                shObj.div_yield_flag = Convert.ToBoolean(partsOfRow[18]);
                shObj.share_type = partsOfRow[19];
                shObj.min_price_increment = (float)Convert.ToDouble(partsOfRow[20]);
                shObj.api_trade_available_flag = Convert.ToBoolean(partsOfRow[21]);
                shObj.uid = partsOfRow[22];
                shObj.real_exchange = partsOfRow[23];
                shObj.position_uid = partsOfRow[24];
                shObj.for_iis_flag = Convert.ToBoolean(partsOfRow[25]);
                shObj.for_qual_investor_flag = Convert.ToBoolean(partsOfRow[26]);
                shObj.weekend_flag = Convert.ToBoolean(partsOfRow[27]);
                shObj.blocked_tca_flag = Convert.ToBoolean(partsOfRow[28]);
                if (shObj.ticker.Equals("MSRS"))
                    Console.WriteLine(shObj.ticker);

                if (partsOfRow[29].Length < 1)
                {
                    shObj.UnavailableForAnalysys = false;
                }
                else
                {
                    shObj.UnavailableForAnalysys = true;
                }
                shObjList.Add(shObj);
            }
            return shObjList;
        }

        /// <summary>
        /// Модуль открытия новых сделок
        /// </summary>
        /// <param name="strategyName"></param>
        /// <param name="scaleName"></param>
        /// <param name="connectionString"></param>
        /// <param name="shares"></param>
        private static void MA_Strategy_OpenTrade(string strategyName, string scaleName, string connectionString, List<ShareObject> shares)
        {
            log.Info("Получаю максимальный CandleId по каждому активу (cfg_last_candles_for_strategy)");
            GetFigiLastCandleId(strategyName, scaleName, connectionString, shares);
            log.Info("Получаю таргеты для сделок");
            List<TradeTargetObject> tradeTargetObjects = GetTargetsForTrades(connectionString);
            log.Info("Получаю свечи для анализа согласно стратегии" + STRATEGY_NAME);
            GetCandlesSMARows(scaleName, connectionString, shares); //в коллекцию shares (в каждый элемент) добавляем колелкцию свечей для проведения анализа по стратегии MA

            //Анализируем на предмет пересечения быстрой и медленной линии
            log.Info("Запуск стратегии " + STRATEGY_NAME);
            foreach (var share in shares)
            {

                log.Info("Работаю над активом " + share.name + "(" + share.ticker + ", " + share.figi + ")");
                List<TradeObject> tradeObjectList = new List<TradeObject>();
                //Проверяем каждую свечу на предмет пробоя короткой линией длинной линии. При этом учитываем значение предыдущей свечи. Если короткая

                for (int i = 1; i < share.candleForSMAStratAnalysisList.Count; i++)
                {

                    int curr_str = i;
                    int prev_str = i - 1;
                    float prevValue = 0;
                    float currValue = 0;

                    log.Info("Проверяю свечу " + share.candleForSMAStratAnalysisList[curr_str]);


                    try
                    {
                        prevValue = share.candleForSMAStratAnalysisList[prev_str].fastInterval - share.candleForSMAStratAnalysisList[prev_str].slowInterval;
                        currValue = share.candleForSMAStratAnalysisList[curr_str].fastInterval - share.candleForSMAStratAnalysisList[curr_str].slowInterval;
                        log.Info("Предыдущая разница между" + STRATEGY_NAME + " = " + prevValue);
                        log.Info("Текущая разница между" + STRATEGY_NAME + " = " + currValue);
                    }
                    catch (Exception ex)
                    {
                        log.Error("Не могу вычислить переменные для стратегии " + strategyName + ". анализируемая свеча " + share.candleForSMAStratAnalysisList[curr_str].candleId);
                        log.Error(ex.ToString());
                    }
                    //Поиск точки входа в сделку LONG
                    if (prevValue <= 0)
                    {
                        if (currValue > 0)
                        {
                            log.Info("Выявлена LONG рекомендация по активу " + share.name + "(" + share.ticker + ", " + share.figi + " свеча входа " + share.candleForSMAStratAnalysisList[curr_str].candleId);
                            try
                            {
                                TradeObject tradeObject = new TradeObject();
                                tradeObject.shareObject = share;
                                tradeObject.tradeId = Guid.NewGuid().ToString().Replace("-", "");
                                tradeObject.tradeType = "LONG";
                                tradeObject.stratName = strategyName;
                                tradeObject.openCandleId = share.candleForSMAStratAnalysisList[i].candleId;
                                tradeObject.openCandleDt = share.candleForSMAStratAnalysisList[i].candleOpenDt;
                                tradeObject.figi = share.figi;
                                tradeObject.tradeStartDt = DateTime.Now;
                                tradeObject.openTradePrice = share.candleForSMAStratAnalysisList[i].closePrice;
                                tradeObject.maxTradePrice = share.candleForSMAStratAnalysisList[i].maxPrice;
                                tradeObject.minTradePrice = share.candleForSMAStratAnalysisList[i].minPrice;

                                tradeObject.maxtradepricecandleid = tradeObject.openCandleId;
                                tradeObject.maxtradepricecandledt = tradeObject.openCandleDt;
                                tradeObject.mintradepricecandleid = tradeObject.openCandleId;
                                tradeObject.mintradepricecandledt = tradeObject.openCandleDt;
                                tradeObject.target1Value = tradeTargetObjects.FirstOrDefault(f => f.tradeType == tradeObject.tradeType & f.figi == share.figi & f.stratname == strategyName).target_1;
                                tradeObject.target2Value = tradeTargetObjects.FirstOrDefault(f => f.tradeType == tradeObject.tradeType & f.figi == share.figi & f.stratname == strategyName).target_2;
                                tradeObject.stopLoss1Value = tradeTargetObjects.FirstOrDefault(f => f.tradeType == tradeObject.tradeType & f.figi == share.figi & f.stratname == strategyName).stop_loss;
                                tradeObject.stopLoss2Value = tradeTargetObjects.FirstOrDefault(f => f.tradeType == tradeObject.tradeType & f.figi == share.figi & f.stratname == strategyName).stop_loss;

                                tradeObject.trade_is_close_analytic = false;
                                tradeObject.trade_is_close_communication = false;

                                tradeObject.target1CloseCause = "OPEN";
                                tradeObject.target2CloseCause = "OPEN";
                                tradeObject.tradeDuration = tradeTargetObjects.FirstOrDefault(f => f.tradeType == tradeObject.tradeType & f.figi == share.figi & f.stratname == strategyName).target_2_duration;

                                tradeObjectList.Add(tradeObject);
                            }
                            catch (Exception ex)
                            {
                                log.Error("Ошибка формирования tradeObject по активу " + share.name + "(" + share.ticker + ", " + share.figi + " свеча входа " + share.candleForSMAStratAnalysisList[curr_str].candleId);
                                log.Error(ex);
                            }
                        }
                    }
                    //Поиск точки входа в сделку SHORT
                    if (prevValue >= 0)
                    {
                        if (currValue < 0)
                        {
                            log.Info("Выявлена SHORT рекомендация по активу " + share.name + "(" + share.ticker + ", " + share.figi + " свеча входа " + share.candleForSMAStratAnalysisList[curr_str].candleId);
                            try
                            {
                                TradeObject tradeObject = new TradeObject();
                                tradeObject.shareObject = share;
                                tradeObject.tradeId = Guid.NewGuid().ToString().Replace("-", "");
                                tradeObject.tradeType = "SHORT";
                                tradeObject.stratName = strategyName;
                                tradeObject.openCandleId = share.candleForSMAStratAnalysisList[i].candleId;
                                tradeObject.openCandleDt = share.candleForSMAStratAnalysisList[i].candleOpenDt;
                                tradeObject.figi = share.figi;
                                tradeObject.tradeStartDt = DateTime.UtcNow;
                                tradeObject.openTradePrice = share.candleForSMAStratAnalysisList[i].closePrice;
                                tradeObject.maxTradePrice = share.candleForSMAStratAnalysisList[i].maxPrice;
                                tradeObject.minTradePrice = share.candleForSMAStratAnalysisList[i].minPrice;

                                tradeObject.maxtradepricecandleid = tradeObject.openCandleId;
                                tradeObject.maxtradepricecandledt = tradeObject.openCandleDt;
                                tradeObject.mintradepricecandleid = tradeObject.openCandleId;
                                tradeObject.mintradepricecandledt = tradeObject.openCandleDt;
                                tradeObject.target1Value = tradeTargetObjects.FirstOrDefault(f => f.tradeType == tradeObject.tradeType & f.figi == share.figi & f.stratname == strategyName).target_1;
                                tradeObject.target2Value = tradeTargetObjects.FirstOrDefault(f => f.tradeType == tradeObject.tradeType & f.figi == share.figi & f.stratname == strategyName).target_2;
                                tradeObject.stopLoss1Value = tradeTargetObjects.FirstOrDefault(f => f.tradeType == tradeObject.tradeType & f.figi == share.figi & f.stratname == strategyName).stop_loss;
                                tradeObject.stopLoss2Value = tradeTargetObjects.FirstOrDefault(f => f.tradeType == tradeObject.tradeType & f.figi == share.figi & f.stratname == strategyName).stop_loss;

                                tradeObject.trade_is_close_analytic = false;
                                tradeObject.trade_is_close_communication = false;

                                tradeObject.target1CloseCause = "OPEN";
                                tradeObject.target2CloseCause = "OPEN";
                                tradeObject.tradeDuration = tradeTargetObjects.FirstOrDefault(f => f.tradeType == tradeObject.tradeType & f.figi == share.figi & f.stratname == strategyName).target_2_duration;

                                tradeObjectList.Add(tradeObject);
                                log.Info("SHORT: " + share.ticker + ", CandleID: " + tradeObject.openCandleId + ", Date: " + tradeObject.openCandleDt);
                            }
                            catch (Exception ex)
                            {
                                log.Error("Ошибка формирования tradeObject по активу " + share.name + "(" + share.ticker + ", " + share.figi + " свеча входа " + share.candleForSMAStratAnalysisList[curr_str].candleId);
                                log.Error(ex);
                            }
                        }
                    }
                }
                share.tradeObjects = tradeObjectList;
            }

            //Сохраняем найденные сделки в таблицу


            log.Info("Сохраняю найденные сделки по стратегии: " + strategyName);
            int tradesCount = 0;
            var sharesWithTrades = shares.Where(w => w.tradeObjects.Count > 0).ToList();
            for (int i = 0; i < sharesWithTrades.Count(); i++)
            {
                log.Info("Сохраняю сделки по активу: " + sharesWithTrades[i].name + "(" + sharesWithTrades[i].figi + ")");
                for (int ii = 0; ii < sharesWithTrades[i].tradeObjects.Count; ii++)
                {
                    var currentDT = DateTime.Now.ToString("yyyy-MM-dd");
                    if (sharesWithTrades[i].tradeObjects[ii].openCandleDt.ToString("yyyy-MM-dd") == currentDT)
                    {
                        //проверяем отсутсвие сделки с candle_id равным candle_id уже готовой или на предмет наличия сделки с тем же направлением и по тому же figi и со статусом trade_is_close_communication = false
                        string dbTradesCountCommand = "select count(*) from public.trades where stratname = '" + STRATEGY_NAME + "' and ((opencandleid = " + sharesWithTrades[i].tradeObjects[ii].openCandleId + " and tradetype = '" + sharesWithTrades[i].tradeObjects[ii].tradeType + "') or (figi = '" + sharesWithTrades[i].tradeObjects[ii].figi + "' and tradetype = '" + sharesWithTrades[i].tradeObjects[ii].tradeType + "' and trade_is_close_communication = false))";
                        var dbTradesCountResult = new PgExecuter(connectionString, log).ExecuteScalarQuery(dbTradesCountCommand);
                        //подготавливаем данные для записи в БД
                        if (Convert.ToInt32(dbTradesCountResult) == 0)
                        {
                            string jsonObj = string.Empty;
                            string sqlCommand_analysis = PrepareSaveTradeCommand(sharesWithTrades, i, ii, "for_analysis");
                            var ticker = tradeTargetObjects.FirstOrDefault(f => f.figi.Equals(sharesWithTrades[i].tradeObjects[ii].figi)).ticker.ToString();
                            if (sharesWithTrades[i].tradeObjects[ii].tradeType.Equals("SHORT"))
                            {
                                jsonObj = JSONSerializedTrade(sharesWithTrades[i].tradeObjects[ii], ticker, "OPEN_SHORT_IDEA.txt", TG_CHANNEL_ID);
                            }
                            else
                            {
                                jsonObj = JSONSerializedTrade(sharesWithTrades[i].tradeObjects[ii], ticker, "OPEN_LONG_IDEA.txt", TG_CHANNEL_ID);
                            }


                            //сохраняем информацию о сделках в БД
                            string sqlCommand_Communications = "INSERT INTO public.communications (id,external_id,create_dt,message_content) VALUES ('" + Guid.NewGuid().ToString().Replace("-", "") + "','" + sharesWithTrades[i].tradeObjects[ii].tradeId + "','" + DateTime.Now.ToString() + "','" + jsonObj + "')";
                            new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand_analysis); //таблица сделок для аналитики
                            new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand_Communications); //сделки для каналов коммуникации

                            tradesCount++;
                        }
                    }
                }
                if (shares[i].candleForSMAStratAnalysisList.Count > 0)
                {
                    log.Info("Сохранено " + tradesCount + " сделок");
                    log.Info("Обновляю максимальный candle_id по активу " + sharesWithTrades[i].name + "(" + sharesWithTrades[i].figi + ")");

                    string sqlCommandUpd = "update cfg_last_candles_for_strategy t set candle_id = '" + shares[i].candleForSMAStratAnalysisList.Max(m => m.candleId) + "', candle_dt = '" + sharesWithTrades[i].candleForSMAStratAnalysisList.Max(m => m.candleOpenDt) + "', update_dt = now() where t.figi = '" + sharesWithTrades[i].figi + "' and strategy_name = '" + strategyName + "' and calc_scale = '" + scaleName + "'";
                    log.Debug("Команда для обновления максимального candle_Id и Candle_DT: ");
                    log.Debug(sqlCommandUpd);

                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommandUpd);
                }
            }

        }



        private static string JSONSerializedTrade(TradeObject tradeObject, string ticker, string communicationTemplate, string tg_channel_id)
        {

            string channel_id = tg_channel_id;//tradeObject.stratName.Equals("MA_12/26") ? "-1001669467340" : "-1001906521615";

            string template_id = communicationTemplate;

            //Готовим объект для сериализации в JSON
            var obj = new
            {
                trade_id = tradeObject.tradeId
                ,
                figi = tradeObject.shareObject.figi
                ,
                name = tradeObject.shareObject.name
                ,
                ticker = "#" + tradeObject.shareObject.ticker
                ,
                strat_name = tradeObject.stratName
                ,
                trade_type = tradeObject.tradeType
                ,
                open_price = tradeObject.openTradePrice.ToString()
                ,
                target_price_1 = tradeObject.tradeType.Equals("LONG") ?
                            (tradeObject.openTradePrice + (tradeObject.target1Value * tradeObject.openTradePrice)).ToString()
                                : (tradeObject.openTradePrice - (tradeObject.target1Value * tradeObject.openTradePrice)).ToString()
                ,
                target_perc_1 = (tradeObject.target1Value * 100).ToString()
                ,
                target_price_2 = tradeObject.tradeType.Equals("LONG") ?
                            (tradeObject.openTradePrice + (tradeObject.target2Value * tradeObject.openTradePrice)).ToString()
                                : (tradeObject.openTradePrice - (tradeObject.target2Value * tradeObject.openTradePrice)).ToString()
                ,
                target_perc_2 = (tradeObject.target2Value * 100).ToString()
                ,
                stop_loss_price = tradeObject.tradeType.Equals("LONG") ?
                            (tradeObject.openTradePrice + (tradeObject.stopLoss1Value * tradeObject.openTradePrice)).ToString()
                            : (tradeObject.openTradePrice - (tradeObject.stopLoss1Value * tradeObject.openTradePrice)).ToString()
                ,
                stop_loss_perc = tradeObject.target1CloseCause.Equals("STOP_LOSS") ? (tradeObject.stopLoss1Value * 100).ToString() : ((tradeObject.target1Value) / 2 * 100).ToString() //если сработал стоплосс 2 - доходность расчитывается как таргет профита 1 деленное на 2.
                ,
                trade_dur_forecast = tradeObject.tradeDuration.ToString()
                ,
                communication_channel = "Telegram".ToUpper()
                ,
                channel_id = channel_id
                ,
                message_template_name = template_id
                ,
                stop_loss_price_for_profit_2 = tradeObject.openTradePrice.ToString()
                ,
                tinkoffAvailableShort = tradeObject.shareObject.short_enabled_flag is true ? "Разрешен" : "Запрещен"

            };
            string jsonObj = JsonConvert.SerializeObject(obj);
            return jsonObj;
        }

        /// <summary>
        /// Модуль мониторинга сделок для аналитики
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scaleName"></param>
        private static void MA_Strategy_TradeWorker(string connectionString, string scaleName)
        {
            //получаем список активных сделок
            List<TradeObject> tradeObjectList = GetActiveTrades(connectionString);

            //Обрабатываю каждую сделку и ищу экстремумы + точки выхода
            foreach (var trade in tradeObjectList)
            {
                float target1Price = 0;
                float target2Price = 0;
                float stopLossPrice = 0;
                //Расчитываем фактические цены закрытия сделки для публикации в канал
                if (trade.tradeType.Equals("LONG"))
                {
                    target1Price = trade.openTradePrice + trade.openTradePrice * trade.target1Value;
                    target2Price = trade.openTradePrice + trade.openTradePrice * trade.target2Value;
                    stopLossPrice = trade.openTradePrice - trade.openTradePrice * trade.stopLoss1Value;
                }
                else if (trade.tradeType.Equals("SHORT"))
                {
                    target1Price = trade.openTradePrice - trade.openTradePrice * trade.target1Value;
                    target2Price = trade.openTradePrice - trade.openTradePrice * trade.target2Value;
                    stopLossPrice = trade.openTradePrice - trade.openTradePrice * trade.stopLoss1Value;
                }

                DateTime lastCandle_dateTime = GetFigiLastOpenCandleDt(STRATEGY_NAME, scaleName, connectionString, trade);

                DateTime executeStartDT = DateTime.UtcNow;
                //для каждой сделки нужно найти все последующие свечи
                //получаю список строк для конвертации их в объект candle
                List<string> candlesStringList = GetCandlesSMARows(scaleName, connectionString, trade, lastCandle_dateTime);
                List<CandleForSMAStratAnalysis> candleForSMAStratAnalyses = GetCandlesSMAObjects(candlesStringList);

                //каждую полученную свечу проверяем на удовлетворение условию стратегии (короткая линия пересекает длинную линию сверху вниз)
                for (int i = 1; i < candleForSMAStratAnalyses.Count; i++)
                {

                    //Расчитываются показатели для аналитической части сделки
                    if (trade.closeCandleId < 1)
                    {
                        var prevValue = candleForSMAStratAnalyses[i - 1].fastInterval - candleForSMAStratAnalyses[i - 1].slowInterval;
                        var currValue = candleForSMAStratAnalyses[i].fastInterval - candleForSMAStratAnalyses[i].slowInterval;

                        //Обновляем минимальные и максимальные цены внутри сделки
                        //Максимальная цена
                        if (trade.maxTradePrice < candleForSMAStratAnalyses[i].maxPrice)
                        {
                            trade.maxTradePrice = candleForSMAStratAnalyses[i].maxPrice;
                            trade.maxtradepricecandleid = candleForSMAStratAnalyses[i].candleId;
                            trade.maxtradepricecandledt = candleForSMAStratAnalyses[i].candleOpenDt;
                        }

                        //Минимальная цена
                        if (trade.minTradePrice > candleForSMAStratAnalyses[i].minPrice)
                        {
                            trade.minTradePrice = candleForSMAStratAnalyses[i].minPrice;
                            trade.mintradepricecandleid = candleForSMAStratAnalyses[i].candleId;
                            trade.mintradepricecandledt = candleForSMAStratAnalyses[i].candleOpenDt;
                        }

                        UpdateTrade(trade, connectionString);

                        //Проверяем на LONG условия короткая должна пересечь длинную сверху вниз
                        if (trade.tradeType.Equals("LONG"))
                        {
                            if (prevValue > 0)
                            {
                                if (currValue <= 0)
                                {
                                    //закрываем LONG сделку
                                    trade.closeCandleId = candleForSMAStratAnalyses[i].candleId;
                                    trade.closeCandleDt = candleForSMAStratAnalyses[i].candleOpenDt;
                                    trade.tradeCloseDt = DateTime.Now;
                                    trade.closeTradePrice = candleForSMAStratAnalyses[i].closePrice;
                                    trade.trade_is_close_analytic = true;
                                    CloseTrade(trade, connectionString); //Закрываем сделку

                                    log.Info("Сделка " + trade.tradeType + " ID:" + trade.tradeId + " закрыта");
                                    break;
                                }
                            }
                        }
                        //Проверяем на SHORT условия - короткая должна пересечь длинную снизу вверх
                        else if (trade.tradeType.Equals("SHORT"))
                        {
                            if (prevValue < 0)
                            {
                                if (currValue >= 0)
                                {
                                    //закрываем SHORT сделку
                                    trade.closeCandleId = candleForSMAStratAnalyses[i].candleId;
                                    trade.closeCandleDt = candleForSMAStratAnalyses[i].candleOpenDt;
                                    trade.tradeCloseDt = DateTime.Now;
                                    trade.closeTradePrice = candleForSMAStratAnalyses[i].closePrice;
                                    trade.trade_is_close_analytic = true;
                                    log.Info("Сделка " + trade.tradeType + " ID:" + trade.tradeId + " закрыта");
                                    CloseTrade(trade, connectionString); //Закрываем сделку
                                    break;
                                }
                            }
                        }
                    }

                    //Проверка достижения таргетов для реальной сделки
                    if (trade.target2CloseCause == null)
                    {
                        float currPrice = candleForSMAStratAnalyses[i].closePrice;
                        if (trade.tradeType.Equals("LONG"))
                        {
                            if (currPrice >= target1Price)
                            {
                                //Записываем инфо о закрытии сделки по первому таргету

                            }
                        }
                        else if (trade.tradeType.Equals("SHORT"))
                        {

                        }
                        //float target1Price = trade.openTradePrice + trade.openTradePrice * trade.target1Value;
                        //float target2Price = trade.openTradePrice + trade.openTradePrice * trade.target2Value;
                        //float stopLossPrice = trade.openTradePrice - trade.openTradePrice * trade.stopLoss1Value;
                    }

                    string sqlCommandUpd = "update cfg_last_candles_for_strategy t set candle_id = '"
                        + candleForSMAStratAnalyses[i].candleId.ToString()
                        + "', candle_dt = '" + candleForSMAStratAnalyses[i].candleOpenDt
                        + "', update_dt = now() where t.figi = '"
                        + candleForSMAStratAnalyses[i].figi + "' and strategy_name = '"
                        + STRATEGY_NAME + "' and calc_scale = '" + scaleName + "'";
                    log.Debug("Команда для обновления максимального candle_Id и Candle_DT: ");
                    log.Debug(sqlCommandUpd);

                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommandUpd);

                }
                DateTime executeFinishDT = DateTime.UtcNow;
                log.Debug("Время выполнения: " + (executeFinishDT - executeStartDT).TotalSeconds + " секунд.");



            }
        }

        /// <summary>
        /// Модуль мониторинга сделок для публикации в канале
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scaleName"></param>
        private static void MA_Strategy_TradeWorker_TradesForChannel(string connectionString, string scaleName, List<ShareObject> shares)
        {
            //получаем список активных сделок
            // List<TradeObject> tradeObjectList = GetActiveTrades(connectionString);
            log.Info("Получаю список активных рекомендаций");
            List<TradeObject> trades = GetActiveTrades(connectionString, true).Where(w => w.trade_is_close_communication is false).OrderBy(o => o.openCandleDt).ToList();
            log.Info("Получено " + trades.Count + " активных рекомендаций");
            foreach (var trade in trades)
            {
                float target1Price = 0;
                float target2Price = 0;
                float stopLossPrice = 0;
                trade.shareObject = shares.First(f => f.figi.Equals(trade.figi));

                //обработка аналитической части сделки
                if (trade.trade_is_close_analytic is false)
                {
                    //критерий закрытия аналитической сделки: короткая линия пересекает длинную сверху вниз
                }
                //обработка торговой части сделки
                if (trade.trade_is_close_communication is false)
                {
                    log.Info("Работаю над рекомендацией: " + trade.tradeId);
                    log.Info("\tТип рекомендации: " + trade.tradeType);
                    //критерий закрытия торговой сделки: достижения таргета target2 или закрытие по стопу (первому или второму)
                    //для каждой сделки нужно найти все последующие свечи
                    //получаю список строк для конвертации их в объект candle
                    log.Debug("Получаю список свечей для анализа цен и точек выхода из рекомендации " + trade.tradeId);
                    List<string> candlesStringList = GetCandlesSMARows(scaleName, connectionString, trade, trade.openCandleDt);
                    List<CandleForSMAStratAnalysis> candleForSMAStratAnalyses = GetCandlesSMAObjects(candlesStringList);
                    log.Debug("Получено " + candleForSMAStratAnalyses.Count + "свечей");

                    //Рассчитываем фактические цены закрытия сделки для публикации в канал
                    if (trade.tradeType.Equals("LONG"))
                    {
                        target1Price = trade.openTradePrice + trade.openTradePrice * trade.target1Value;
                        target2Price = trade.openTradePrice + trade.openTradePrice * trade.target2Value;
                        if (trade.target1CloseCause.Equals("OPEN"))
                            stopLossPrice = trade.openTradePrice + trade.openTradePrice * trade.stopLoss1Value;//значение stopLoss1Value должно быть отрицательным!!!!
                        else if (trade.target2CloseCause.Equals("OPEN"))
                            stopLossPrice = trade.openTradePrice + trade.openTradePrice * trade.stopLoss2Value;
                    }
                    else if (trade.tradeType.Equals("SHORT"))
                    {
                        target1Price = trade.openTradePrice - trade.openTradePrice * trade.target1Value;
                        target2Price = trade.openTradePrice - trade.openTradePrice * trade.target2Value;
                        if (trade.target1CloseCause.Equals("OPEN"))
                            stopLossPrice = trade.openTradePrice - trade.openTradePrice * trade.stopLoss1Value;
                        else if (trade.target2CloseCause.Equals("OPEN"))
                            stopLossPrice = trade.openTradePrice - trade.openTradePrice * trade.stopLoss2Value;
                    }

                    var candleList = candleForSMAStratAnalyses.Where(w => w.candleOpenDt.ToString("yyyy-MM-dd").Equals(DateTime.Now.ToString("yyyy-MM-dd"))).ToList();
                    //Обрабатываем каждую свечу из полученных для текущей сделки
                    foreach (var candle in candleList)
                    {
                        log.Info("Обрабатываю свечу: " + candle.candleId + " для figi = " + candle.figi);
                        //Обновляем минимальные и максимальные цены внутри сделки
                        //Максимальная цена
                        if (trade.maxTradePrice < candle.maxPrice)
                        {
                            trade.maxTradePrice = candle.maxPrice;
                            trade.maxtradepricecandleid = candle.candleId;
                            trade.maxtradepricecandledt = candle.candleOpenDt;
                        }

                        //Минимальная цена
                        if (trade.minTradePrice > candle.minPrice)
                        {
                            trade.minTradePrice = candle.minPrice;
                            trade.mintradepricecandleid = candle.candleId;
                            trade.mintradepricecandledt = candle.candleOpenDt;
                        }

                        UpdateTrade(trade, connectionString);

                        //Вычисляем последнюю актуальную цену обрабатываемой свечи

                        float currMaxPrice = candle.maxPrice;
                        float currMinPrice = candle.minPrice;
                        float currClosePrice = candle.closePrice;


                        bool result_1_target = false;
                        bool result_2_target = false;
                        bool result_stopLoss = false;
                        log.Info("Проверяю сработку таргетов и стопов");
                        log.Info("Тип рекомендации: " + trade.tradeType);
                        if (trade.tradeType.Equals("LONG"))
                        {
                            log.Info("Выполняю проверки для LONG рекомендаций");
                            if ((currClosePrice >= target1Price || currMaxPrice >= target1Price) & !trade.target1CloseCause.Equals("PROFIT"))
                            {
                                log.Info("\tTarget 1: (currClosePrice(" + currClosePrice + ") >= target1Price(" + target1Price + ") или currMaxPrice(" + currMaxPrice + ") >= target1Price(" + target1Price + ")) и target1CloseCause != PROFIT (" + trade.target1CloseCause + ")");
                                result_1_target = true;
                                log.Info("\t\tРезультат вычисления = " + result_1_target);

                            }
                            if ((currClosePrice >= target2Price || currMaxPrice >= target2Price) & !trade.target2CloseCause.Equals("PROFIT"))
                            {
                                log.Info("\tTarget 2: (currClosePrice(" + currClosePrice + ") >= target2Price(" + target2Price + ") или currMaxPrice(" + currMaxPrice + ") >= target2Price(" + target2Price + ")) и target2CloseCause != PROFIT (" + trade.target2CloseCause + ")");
                                result_2_target = true;
                                log.Info("\t\tРезультат вычисления = " + result_2_target);
                            }
                            if (currClosePrice <= stopLossPrice || currMinPrice <= stopLossPrice)
                            {
                                log.Info("\tcurrClosePrice(" + currClosePrice + ") <= stopLossPrice(" + stopLossPrice + ")");
                                result_stopLoss = true;
                                log.Info("\t\tРезультат вычисления = " + result_stopLoss);
                            }

                        }
                        else if (trade.tradeType.Equals("SHORT"))
                        {
                            log.Info("Выполняю проверки для SHORT рекомендаций");
                            if ((currClosePrice <= target1Price || currMinPrice <= target1Price) & !trade.target1CloseCause.Equals("PROFIT"))
                            {
                                log.Info("\tTarget 1: (currClosePrice(" + currClosePrice + ") <= target1Price(" + target1Price + ") или currMaxPrice(" + currMaxPrice + ") <= target1Price(" + target1Price + ")) и target1CloseCause != PROFIT (" + trade.target1CloseCause + ")");
                                result_1_target = true;
                                log.Info("\t\tРезультат вычисления = " + result_1_target);
                            }
                            if ((currClosePrice <= target2Price || currMinPrice <= target2Price) & !trade.target2CloseCause.Equals("PROFIT"))
                            {
                                log.Info("\tTarget 2: (currClosePrice(" + currClosePrice + ") <= target2Price(" + target2Price + ") или currMaxPrice(" + currMaxPrice + ") <= target2Price(" + target2Price + ")) и target2CloseCause != PROFIT (" + trade.target2CloseCause + ")");
                                result_2_target = true;
                                log.Info("\t\tРезультат вычисления = " + result_2_target);
                            }
                            if (currClosePrice >= stopLossPrice || currMaxPrice >= stopLossPrice)
                            {
                                log.Info("\tcurrClosePrice(" + currClosePrice + ") >= stopLossPrice(" + stopLossPrice + ")");
                                result_stopLoss = true;
                                log.Info("\t\tРезультат вычисления = " + result_stopLoss);
                            }
                        }

                        log.Info("currMaxPrice: " + currMaxPrice);
                        log.Info("currClosePrice: " + currClosePrice);
                        log.Info("currMinPrice: " + currMinPrice);
                        log.Info("Target 1 (" + target1Price + "): " + result_1_target);
                        log.Info("Target 2 (" + target2Price + "): " + result_2_target);
                        log.Info("StopLoss (" + stopLossPrice + "): " + result_stopLoss);

                        //проверяем, что Target1 еще не закрыт (проверка по полю target1CloseCause)
                        if (!trade.target1CloseCause.Equals("PROFIT") & !trade.target1CloseCause.Equals("STOP_LOSS"))
                        {
                            //Проверяем достижение первой цели
                            if (result_1_target)
                            //if (target1Price >= currMinPrice & target1Price <= currMaxPrice & !trade.target1CloseCause.Equals("PROFIT"))
                            {

                                log.Info("ПРОФИТ 1 достигнут!");
                                //Обновляем информацию в сделке - достижение таргета и новый StopLoss
                                trade.target1ClosePrice = target1Price;
                                trade.target1CloseDT = candle.candleOpenDt;
                                trade.target1CloseCause = "PROFIT";
                                trade.stopLoss2Value = float.Parse("0,001");

                                string sqlCommand = "UPDATE public.trades t SET target1ClosePrice = " + trade.target1ClosePrice.ToString().Replace(',', '.')
                                    + ", target1CloseDT = '" + trade.target1CloseDT.ToString()
                                    + "', target1CloseCause = '" + trade.target1CloseCause
                                    + "', stopLoss2Value = " + trade.stopLoss2Value.ToString().Replace(',', '.') + " WHERE t.tradeid = '" + trade.tradeId + "'";

                                string ticker = new PgExecuter(connectionString, log).ExecuteScalarQuery("SELECT TICKER FROM SHARES WHERE FIGI = '" + trade.figi + "'");
                                string jsonObj = JSONSerializedTrade(trade, ticker, "PROFIT_1_DONE.txt", TG_CHANNEL_ID);

                                string sqlCommand_Communications = "INSERT INTO public.communications (id,external_id,create_dt,message_content) VALUES ('" + Guid.NewGuid().ToString().Replace("-", "") + "','" + trade.tradeId + "','" + DateTime.Now.ToString() + "','" + jsonObj + "')";

                                new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand_Communications);
                                new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand);
                            }

                            //Проверяем достижение второй цели. Если цель достигнута - закрываем рекомендацию
                            if (result_2_target)
                            {
                                //ПРОФИТ 2 достигнут
                                log.Info("ПРОФИТ 2 достигнут!");
                                //Обновляем информацию в сделке - достижение таргета и новый StopLoss
                                trade.target2ClosePrice = target2Price;
                                trade.target2CloseDT = candle.candleOpenDt;
                                trade.target2CloseCause = "PROFIT";
                                trade.trade_is_close_communication = true;


                                string sqlCommand = "UPDATE public.trades t SET target2ClosePrice = " + trade.target2ClosePrice.ToString().Replace(',', '.')
                                    + ", target2CloseDT = '" + trade.target2CloseDT.ToString()
                                    + "', target2CloseCause = '" + trade.target2CloseCause
                                    + "', trade_is_close_communication = '" + trade.trade_is_close_communication
                                    + "' WHERE t.tradeid = '" + trade.tradeId + "'";

                                string ticker = new PgExecuter(connectionString, log).ExecuteScalarQuery("SELECT TICKER FROM SHARES WHERE FIGI = '" + trade.figi + "'");
                                string jsonObj = JSONSerializedTrade(trade, ticker, "PROFIT_2_CLOSE_TRADE.txt", TG_CHANNEL_ID);

                                string sqlCommand_Communications = "INSERT INTO public.communications (id,external_id,create_dt,message_content) VALUES ('" + Guid.NewGuid().ToString().Replace("-", "") + "','" + trade.tradeId + "','" + DateTime.Now.ToString() + "','" + jsonObj + "')";



                                new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand_Communications);

                                new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand);
                                break;
                            }

                            //Проверяем на срабатывание StopLoss
                            if (result_stopLoss)
                            {
                                //Проверяем какой именно стоп сработал (от первой цели или от второй?)

                                //Сработал 1-й стоп: обновляем цифры и закрываем сделку
                                if (!trade.target1CloseCause.Equals("PROFIT"))
                                {

                                    log.Info("Сработал первый стоп лосс");
                                    //Обновляем информацию в сделке - достижение таргета и новый StopLoss
                                    trade.target1ClosePrice = stopLossPrice;
                                    trade.target1CloseDT = candle.candleOpenDt;
                                    trade.target1CloseCause = "STOPLOSS_1";
                                    trade.trade_is_close_communication = true;

                                    string sqlCommand = "UPDATE public.trades t SET target1ClosePrice = " + trade.target1ClosePrice.ToString().Replace(',', '.')
                                        + ", target1CloseDT = '" + trade.target1CloseDT.ToString()
                                        + "', target1CloseCause = '" + trade.target1CloseCause
                                        + "', trade_is_close_communication = '" + trade.trade_is_close_communication
                                        + "' WHERE t.tradeid = '" + trade.tradeId + "'";



                                    string ticker = new PgExecuter(connectionString, log).ExecuteScalarQuery("SELECT TICKER FROM SHARES WHERE FIGI = '" + trade.figi + "'");
                                    string jsonObj = JSONSerializedTrade(trade, ticker, "STOP_LOSS.txt", TG_CHANNEL_ID);

                                    string sqlCommand_Communications = "INSERT INTO public.communications (id,external_id,create_dt,message_content) VALUES ('" + Guid.NewGuid().ToString().Replace("-", "") + "','" + trade.tradeId + "','" + DateTime.Now.ToString() + "','" + jsonObj + "')";

                                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand_Communications);

                                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand);
                                    break;
                                }
                                //Сработал 2-й стоп: обновляем цифры и закрываем сделку
                                else if (!trade.target2CloseCause.Equals("PROFIT"))
                                {
                                    log.Info("Сработал второй стоп лосс");
                                    trade.target2ClosePrice = stopLossPrice;
                                    trade.target2CloseDT = candle.candleOpenDt;
                                    trade.target2CloseCause = "STOPLOSS_2";
                                    trade.trade_is_close_communication = true;


                                    string sqlCommand = "UPDATE public.trades t SET target2ClosePrice = " + trade.target2ClosePrice.ToString().Replace(',', '.')
                                        + ", target2CloseDT = '" + trade.target2CloseDT.ToString()
                                        + "', target2CloseCause = '" + trade.target2CloseCause
                                         + "', trade_is_close_communication = '" + trade.trade_is_close_communication
                                        + "' WHERE t.tradeid = '" + trade.tradeId + "'";


                                    string ticker = new PgExecuter(connectionString, log).ExecuteScalarQuery("SELECT TICKER FROM SHARES WHERE FIGI = '" + trade.figi + "'");
                                    string jsonObj = JSONSerializedTrade(trade, ticker, "STOP_LOSS.txt", TG_CHANNEL_ID);

                                    string sqlCommand_Communications = "INSERT INTO public.communications (id,external_id,create_dt,message_content) VALUES ('" + Guid.NewGuid().ToString().Replace("-", "") + "','" + trade.tradeId + "','" + DateTime.Now.ToString() + "','" + jsonObj + "')";

                                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand_Communications);


                                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand);
                                    break;
                                }

                            }
                        }
                        // если первый таргет был достигнут - проверяем второй
                        else if (!trade.target2CloseCause.Equals("PROFIT") & !trade.target2CloseCause.Equals("STOP_LOSS"))
                        {
                            //Проверяем достижение второй цели. Если цель достигнута - закрываем сделку
                            if (result_2_target)
                            {
                                log.Info("ПРОФИТ 2 достигнут");
                                //Обновляем информацию в сделке - достижение таргета и новый StopLoss
                                trade.target2ClosePrice = target2Price;
                                trade.target2CloseDT = candle.candleOpenDt;
                                trade.target2CloseCause = "PROFIT";
                                trade.trade_is_close_communication = true;


                                string sqlCommand = "UPDATE public.trades t SET target2ClosePrice = " + trade.target2ClosePrice.ToString().Replace(',', '.')
                                    + ", target2CloseDT = '" + trade.target2CloseDT.ToString()
                                    + "', target2CloseCause = '" + trade.target2CloseCause
                                    + "', trade_is_close_communication = '" + trade.trade_is_close_communication
                                    + "' WHERE t.tradeid = '" + trade.tradeId + "'";

                                string ticker = new PgExecuter(connectionString, log).ExecuteScalarQuery("SELECT TICKER FROM SHARES WHERE FIGI = '" + trade.figi + "'");
                                string jsonObj = JSONSerializedTrade(trade, ticker, "PROFIT_2_CLOSE_TRADE.txt", TG_CHANNEL_ID);

                                string sqlCommand_Communications = "INSERT INTO public.communications (id,external_id,create_dt,message_content) VALUES ('" + Guid.NewGuid().ToString().Replace("-", "") + "','" + trade.tradeId + "','" + DateTime.Now.ToString() + "','" + jsonObj + "')";



                                new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand_Communications);

                                new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand);
                                break;
                            }

                            //Проверяем на срабатывание StopLoss
                            if (result_stopLoss) //Решение о фиксации убытка принимается на основании цены закрытия сделки (в том числе и внутри дня)
                            {
                                //Проверяем какой именно стоп сработал (от первой цели или от второй?)

                                //Сработал 1-й стоп: обновляем цифры и закрываем сделку
                                if (!trade.target1CloseCause.Equals("PROFIT"))
                                {
                                    //Сработал первый стоп лосс
                                    log.Info("Сработал первый стоп лосс");
                                    //Обновляем информацию в сделке - достижение таргета и новый StopLoss
                                    trade.target1ClosePrice = stopLossPrice;
                                    trade.target1CloseDT = candle.candleOpenDt;
                                    trade.target1CloseCause = "STOPLOSS_1";
                                    trade.trade_is_close_communication = true;

                                    string sqlCommand = "UPDATE public.trades t SET target1ClosePrice = " + trade.target1ClosePrice.ToString().Replace(',', '.')
                                        + ", target1CloseDT = '" + trade.target1CloseDT.ToString()
                                        + "', target1CloseCause = '" + trade.target1CloseCause
                                        + "', trade_is_close_communication = '" + trade.trade_is_close_communication
                                        + "' WHERE t.tradeid = '" + trade.tradeId + "'";

                                    string ticker = new PgExecuter(connectionString, log).ExecuteScalarQuery("SELECT TICKER FROM SHARES WHERE FIGI = '" + trade.figi + "'");
                                    string jsonObj = JSONSerializedTrade(trade, ticker, "STOP_LOSS.txt", TG_CHANNEL_ID);

                                    string sqlCommand_Communications = "INSERT INTO public.communications (id,external_id,create_dt,message_content) VALUES ('" + Guid.NewGuid().ToString().Replace("-", "") + "','" + trade.tradeId + "','" + DateTime.Now.ToString() + "','" + jsonObj + "')";

                                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand_Communications);

                                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand);
                                    break;
                                }
                                //Сработал 2-й стоп: обновляем цифры и закрываем сделку
                                else if (!trade.target2CloseCause.Equals("PROFIT"))
                                {
                                    //Сработал второй стоп лосс
                                    log.Info("Сработал второй стоп лосс");
                                    trade.target2ClosePrice = stopLossPrice;
                                    trade.target2CloseDT = candle.candleOpenDt;
                                    trade.target2CloseCause = "STOPLOSS_2";
                                    trade.trade_is_close_communication = true;

                                    string sqlCommand = "UPDATE public.trades t SET target2ClosePrice = " + trade.target2ClosePrice.ToString().Replace(',', '.')
                                        + ", target2CloseDT = '" + trade.target2CloseDT.ToString()
                                        + "', target2CloseCause = '" + trade.target2CloseCause
                                         + "', trade_is_close_communication = '" + trade.trade_is_close_communication
                                        + "' WHERE t.tradeid = '" + trade.tradeId + "'";

                                    string ticker = new PgExecuter(connectionString, log).ExecuteScalarQuery("SELECT TICKER FROM SHARES WHERE FIGI = '" + trade.figi + "'");
                                    string jsonObj = JSONSerializedTrade(trade, ticker, "STOP_LOSS.txt", TG_CHANNEL_ID);

                                    string sqlCommand_Communications = "INSERT INTO public.communications (id,external_id,create_dt,message_content) VALUES ('" + Guid.NewGuid().ToString().Replace("-", "") + "','" + trade.tradeId + "','" + DateTime.Now.ToString() + "','" + jsonObj + "')";

                                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand_Communications);

                                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand);
                                    break;
                                }

                            }
                        }

                    }
                }
            }
        }

        private static List<TradeTargetObject> GetTargetsForTrades(string connectionString)
        {
            log.Info("Получаю список таргетов по активам");
            List<TradeTargetObject> tradeTargetObjects = new List<TradeTargetObject>();
            string sqlCommand = "select figi, stratname, target_1, target_2, target_1_duration, target_2_duration, stop_loss_2 as stop_loss, tradetype, ticker from targets_for_trades";
            List<string> stringTargetList = new PgExecuter(connectionString, log).ExecuteReader(sqlCommand);

            foreach (var str in stringTargetList)
            {
                var partsOfRow = str.Split(';');
                TradeTargetObject targetObject = new TradeTargetObject();
                targetObject.figi = partsOfRow[0];
                targetObject.stratname = partsOfRow[1];
                targetObject.target_1 = float.Parse(partsOfRow[2]);
                targetObject.target_2 = float.Parse(partsOfRow[3]);
                targetObject.target_1_duration = float.Parse(partsOfRow[4]);
                targetObject.target_2_duration = float.Parse(partsOfRow[5]);
                targetObject.stop_loss = float.Parse(partsOfRow[6]);
                targetObject.tradeType = partsOfRow[7];
                targetObject.ticker = partsOfRow[8];

                tradeTargetObjects.Add(targetObject);
            }
            log.Info("Список таргетов получен");
            return tradeTargetObjects;
        }

        private static void UpdateTrade(TradeObject trade, string connectionString)
        {
            string sqlCommand = "update public.trades t set maxTradePrice = '" + trade.maxTradePrice.ToString().Replace(',', '.')
                + "', maxtradepricecandleid = " + trade.maxtradepricecandleid
                + ", maxtradepricecandledt = '" + trade.maxtradepricecandledt.ToString()
                + "', minTradePrice = '" + trade.minTradePrice.ToString().Replace(',', '.')
                + "', mintradepricecandleid = " + trade.mintradepricecandleid
                + ", mintradepricecandledt = '" + trade.mintradepricecandledt.ToString() + "' where t.tradeid = '" + trade.tradeId + "'";
            new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand);
        }

        private static void CloseTrade(TradeObject trade, string connectionString)
        {
            string sqlCommand = "update public.trades t set closeCandleId = " + trade.closeCandleId
                + ", closeCandleDt = '" + trade.closeCandleDt.ToString()
                + "', tradeCloseDt = '" + trade.tradeCloseDt.ToString()
                + "', closeTradePrice = '" + trade.closeTradePrice.ToString().Replace(',', '.')
                + "', trade_is_close_analytic = '" + trade.trade_is_close_analytic
                + "' where t.tradeid = '" + trade.tradeId + "'";
            new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand);
        }


        private static List<TradeObject> GetActiveTrades(string connectionString, bool isCommunicationTrades)
        {
            log.Info("Получаю список активных сделок");
            string sqlCommand = "select tradeId,tradeType,stratName,openCandleId,openCandleDt,figi,tradeStartDt,openTradePrice, maxTradePrice, minTradePrice,maxtradepricecandleid,maxtradepricecandledt,mintradepricecandleid,mintradepricecandledt,calculatetype, target1Value, target2Value, stopLoss1Value, stopLoss2Value, target1ClosePrice, target2ClosePrice, target1CloseDT, target2CloseDT, target1CloseCause, target2CloseCause, trade_is_close_analytic, trade_is_close_communication  from trades where trade_is_close_communication is false and stratname = '" + STRATEGY_NAME + "'";
            List<string> tradesStrings = new PgExecuter(connectionString, log).ExecuteReader(sqlCommand);
            List<TradeObject> tradeObjectList = new List<TradeObject>();
            foreach (var str in tradesStrings)
            {
                var partsOfRow = str.Split(';');

                TradeObject tradeObject = new TradeObject();
                tradeObject.tradeId = partsOfRow[0].ToString();
                tradeObject.tradeType = partsOfRow[1].ToString();
                tradeObject.stratName = partsOfRow[2].ToString();
                tradeObject.openCandleId = Convert.ToInt32(partsOfRow[3]);
                tradeObject.openCandleDt = Convert.ToDateTime(partsOfRow[4]);
                tradeObject.figi = partsOfRow[5].ToString();
                tradeObject.tradeStartDt = Convert.ToDateTime(partsOfRow[6]);
                tradeObject.openTradePrice = float.Parse(partsOfRow[7]);
                tradeObject.maxTradePrice = float.Parse(partsOfRow[8]);
                tradeObject.minTradePrice = float.Parse(partsOfRow[9]);
                tradeObject.maxtradepricecandleid = Convert.ToInt32(partsOfRow[10]);
                tradeObject.maxtradepricecandledt = Convert.ToDateTime(partsOfRow[11]);
                tradeObject.mintradepricecandleid = Convert.ToInt32(partsOfRow[12]);
                tradeObject.mintradepricecandledt = Convert.ToDateTime(partsOfRow[13]);
                tradeObject.calculatetype = partsOfRow[14].ToString();
                tradeObject.target1Value = float.Parse(partsOfRow[15]);
                tradeObject.target2Value = float.Parse(partsOfRow[16]);
                tradeObject.stopLoss1Value = float.Parse(partsOfRow[17]);
                tradeObject.stopLoss2Value = float.Parse(partsOfRow[18]);


                if (partsOfRow[19].Length > 0)
                    tradeObject.target1ClosePrice = float.Parse(partsOfRow[19]);
                if (partsOfRow[20].Length > 0)
                    tradeObject.target2ClosePrice = float.Parse(partsOfRow[20]);
                if (partsOfRow[21].Length > 0)
                    tradeObject.target1CloseDT = Convert.ToDateTime(partsOfRow[21]);
                if (partsOfRow[22].Length > 0)
                    tradeObject.target2CloseDT = Convert.ToDateTime(partsOfRow[22]);
                if (partsOfRow[23].Length > 0)
                    tradeObject.target1CloseCause = partsOfRow[23].ToString();
                if (partsOfRow[24].Length > 0)
                    tradeObject.target2CloseCause = partsOfRow[24].ToString();

                tradeObject.trade_is_close_analytic = Convert.ToBoolean(partsOfRow[25].ToString());
                tradeObject.trade_is_close_communication = Convert.ToBoolean(partsOfRow[26].ToString());

                tradeObjectList.Add(tradeObject);
            }
            log.Info("Найдено " + tradeObjectList.Count + " активных сделок");
            return tradeObjectList;
        }

        private static List<TradeObject> GetActiveTrades(string connectionString)
        {
            log.Info("Получаю список активных сделок");
            string sqlCommand = "select tradeId,tradeType,stratName,openCandleId,openCandleDt,figi,tradeStartDt,openTradePrice, maxTradePrice, minTradePrice,maxtradepricecandleid,maxtradepricecandledt,mintradepricecandleid,mintradepricecandledt,calculatetype, target1Value, target2Value, stopLoss1Value, stopLoss2Value, target1ClosePrice, target2ClosePrice, target1CloseDT, target2CloseDT, target1CloseCause, target2CloseCause, trade_is_close_analytic, trade_is_close_communication  from trades where (closecandleid is null or target2closedt is null) and stratname = '" + STRATEGY_NAME + "'";
            List<string> tradesStrings = new PgExecuter(connectionString, log).ExecuteReader(sqlCommand);
            List<TradeObject> tradeObjectList = new List<TradeObject>();
            foreach (var str in tradesStrings)
            {
                var partsOfRow = str.Split(';');

                TradeObject tradeObject = new TradeObject();
                tradeObject.tradeId = partsOfRow[0].ToString();
                tradeObject.tradeType = partsOfRow[1].ToString();
                tradeObject.stratName = partsOfRow[2].ToString();
                tradeObject.openCandleId = Convert.ToInt32(partsOfRow[3]);
                tradeObject.openCandleDt = Convert.ToDateTime(partsOfRow[4]);
                tradeObject.figi = partsOfRow[5].ToString();
                tradeObject.tradeStartDt = Convert.ToDateTime(partsOfRow[6]);
                tradeObject.openTradePrice = float.Parse(partsOfRow[7]);
                tradeObject.maxTradePrice = float.Parse(partsOfRow[8]);
                tradeObject.minTradePrice = float.Parse(partsOfRow[9]);
                tradeObject.maxtradepricecandleid = Convert.ToInt32(partsOfRow[10]);
                tradeObject.maxtradepricecandledt = Convert.ToDateTime(partsOfRow[11]);
                tradeObject.mintradepricecandleid = Convert.ToInt32(partsOfRow[12]);
                tradeObject.mintradepricecandledt = Convert.ToDateTime(partsOfRow[13]);
                tradeObject.calculatetype = partsOfRow[14].ToString();
                tradeObject.target1Value = float.Parse(partsOfRow[15]);
                tradeObject.target2Value = float.Parse(partsOfRow[16]);
                tradeObject.stopLoss1Value = float.Parse(partsOfRow[17]);
                tradeObject.stopLoss2Value = float.Parse(partsOfRow[18]);


                if (partsOfRow[19].Length > 0)
                    tradeObject.target1ClosePrice = float.Parse(partsOfRow[19]);
                if (partsOfRow[20].Length > 0)
                    tradeObject.target2ClosePrice = float.Parse(partsOfRow[20]);
                if (partsOfRow[21].Length > 0)
                    tradeObject.target1CloseDT = Convert.ToDateTime(partsOfRow[21]);
                if (partsOfRow[22].Length > 0)
                    tradeObject.target2CloseDT = Convert.ToDateTime(partsOfRow[22]);
                if (partsOfRow[23].Length > 0)
                    tradeObject.target1CloseCause = partsOfRow[23].ToString();
                if (partsOfRow[24].Length > 0)
                    tradeObject.target2CloseCause = partsOfRow[24].ToString();

                tradeObject.trade_is_close_analytic = Convert.ToBoolean(partsOfRow[25].ToString());
                tradeObject.trade_is_close_communication = Convert.ToBoolean(partsOfRow[26].ToString());

                tradeObjectList.Add(tradeObject);
            }
            log.Info("Найдено " + tradeObjectList.Count + " активных сделок");
            return tradeObjectList;
        }

        private static string PrepareSaveTradeCommand(List<ShareObject> shares, int ShareCycleNum, int TradeCycleNum, string tradeType)
        {


            string sqlCommand = "INSERT INTO public.trades (tradeId,tradeType,stratName,openCandleId,openCandleDt,figi,tradeStartDt,openTradePrice,maxTradePrice,minTradePrice,maxtradepricecandleid,maxtradepricecandledt,mintradepricecandleid,mintradepricecandledt,calculatetype,target1Value,target2Value,stopLoss1Value,stopLoss2Value, trade_is_close_analytic, trade_is_close_communication, target1CloseCause, target2CloseCause) VALUES('"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].tradeId + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].tradeType + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].stratName + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].openCandleId + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].openCandleDt + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].figi + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].tradeStartDt + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].openTradePrice.ToString().Replace(',', '.') + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].maxTradePrice.ToString().Replace(',', '.') + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].minTradePrice.ToString().Replace(',', '.') + "',"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].maxtradepricecandleid + ",'"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].maxtradepricecandledt + "',"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].mintradepricecandleid + ",'"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].mintradepricecandledt + "','"
                + tradeType + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].target1Value.ToString().Replace(',', '.') + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].target2Value.ToString().Replace(',', '.') + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].stopLoss1Value.ToString().Replace(',', '.') + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].stopLoss2Value.ToString().Replace(',', '.') + "',"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].trade_is_close_analytic + ","
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].trade_is_close_communication + ",'"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].target1CloseCause + "','"
                + shares[ShareCycleNum].tradeObjects[TradeCycleNum].target2CloseCause + "')";

            log.Debug("Команда для сохранения сделки: ");
            log.Debug(sqlCommand);
            return sqlCommand;
        }

        private static void GetFigiLastCandleId(string strategyName, string scaleName, string connectionString, List<ShareObject> shares)
        {
            string getLastCandleIdComm = string.Empty;
            for (int i = 0; i < shares.Count; i++)
            {
                getLastCandleIdComm = "select candle_id from public.cfg_last_candles_for_strategy where strategy_name = '" + strategyName + "' and calc_scale = '" + scaleName + "' and figi = '" + shares[i].figi + "'";
                log.Info(getLastCandleIdComm);
                var res = new PgExecuter(connectionString, log).ExecuteScalarQuery(getLastCandleIdComm);
                shares[i].LastCandleIdForStrategy = Convert.ToInt32(res);
            }
        }

        private static DateTime GetFigiLastOpenCandleDt(string strategyName, string scaleName, string connectionString, TradeObject trade)
        {
            DateTime return_candle_dt = DateTime.MinValue;

            string getLastCandleIdComm = string.Empty;
            getLastCandleIdComm = "select candle_dt from public.cfg_last_candles_for_strategy where strategy_name = '" + strategyName + "' and calc_scale = '" + scaleName + "' and figi = '" + trade.figi + "'";
            log.Info(getLastCandleIdComm);
            var res = new PgExecuter(connectionString, log).ExecuteScalarQuery(getLastCandleIdComm);

            return_candle_dt = Convert.ToDateTime(res);
            return return_candle_dt;


        }

        private static List<CandleForSMAStratAnalysis> GetCandlesSMAObjects(List<string> shareCandlesFoAnalysisStrings)
        {
            List<CandleForSMAStratAnalysis> candleForSMAStratAnalysisList = new List<CandleForSMAStratAnalysis>();
            foreach (var str in shareCandlesFoAnalysisStrings)
            {
                var partsOfRow = str.Split(';');
                CandleForSMAStratAnalysis candleForSMAStratAnalysis = new CandleForSMAStratAnalysis();
                candleForSMAStratAnalysis.candleId = Convert.ToInt32(partsOfRow[0]);
                candleForSMAStratAnalysis.figi = partsOfRow[1];
                candleForSMAStratAnalysis.candleOpenDt = Convert.ToDateTime(partsOfRow[2]);
                candleForSMAStratAnalysis.fastInterval = float.Parse(partsOfRow[3]);
                candleForSMAStratAnalysis.slowInterval = float.Parse(partsOfRow[4]);
                candleForSMAStratAnalysis.openPrice = float.Parse(partsOfRow[5]);
                candleForSMAStratAnalysis.closePrice = float.Parse(partsOfRow[6]);
                candleForSMAStratAnalysis.minPrice = float.Parse(partsOfRow[7]);
                candleForSMAStratAnalysis.maxPrice = float.Parse(partsOfRow[8]);

                candleForSMAStratAnalysisList.Add(candleForSMAStratAnalysis);
            }
            return candleForSMAStratAnalysisList;
        }

        private static void GetCandlesSMARows(string scaleName, string connectionString, List<ShareObject> shares)
        {
            log.Info("Требуется обработать: " + shares.Count);
            string getCandlesForAnalisys = string.Empty;
            for (int i = 0; i < shares.Count; i++)
            {
                //getCandlesForAnalisys = "select id, figi, candle_start_dt_utc, interval_" + FAST_INTERVAL + ", interval_" + SLOW_INTERVAL + ", open_price, close_price ,min_price, max_price  from public.union_history_candles_all_scales uhcas join union_candles_all_intervals ucai on uhcas.id = ucai.candle_id where ucai.calculate_type = 'MOVING_AVG_CLOSE'  and uhcas.scale = '" + scaleName + "'  and uhcas.figi = '" + shares[i].figi + "' and uhcas.id >= " + shares[i].LastCandleIdForStrategy + " order by uhcas.candle_start_dt_utc";
                getCandlesForAnalisys = "select id, figi, candle_start_dt_utc, interval_" + FAST_INTERVAL + ", interval_" + SLOW_INTERVAL + ", open_price, close_price ,min_price, max_price  from public.union_history_candles_all_scales uhcas join union_candles_all_intervals ucai on uhcas.id = ucai.candle_id where ucai.calculate_type = 'MOVING_AVG_CLOSE'  and uhcas.scale = '" + scaleName + "'  and uhcas.figi = '" + shares[i].figi + "' and uhcas.candle_start_dt_utc >= '" + DateTime.Now.AddDays(-7).ToString("yyyy-MM-dd") + "' order by uhcas.candle_start_dt_utc";
                List<string> shareCandlesFoAnalysisStrings = new PgExecuter(connectionString, log).ExecuteReader(getCandlesForAnalisys);

                shares[i].candleForSMAStratAnalysisList = GetCandlesSMAObjects(shareCandlesFoAnalysisStrings);
                log.Info("Осталось обработать: " + (shares.Count - (i + 1)).ToString());
            }
        }

        private static List<string> GetCandlesSMARows(string scaleName, string connectionString, TradeObject tradeObject, DateTime lastCandle_dateTime)
        {

            string getCandlesForAnalisys = string.Empty;
            //Тут нужно переррабатывать - нужен онлайн поток по открытым сделкам
            getCandlesForAnalisys = "select id, figi, candle_start_dt_utc, interval_" + FAST_INTERVAL + ", interval_" + SLOW_INTERVAL + ", open_price, close_price ,min_price, max_price  from public.union_history_candles_all_scales uhcas join union_candles_all_intervals ucai on uhcas.id = ucai.candle_id where ucai.calculate_type = 'MOVING_AVG_CLOSE'  and uhcas.scale = '" + scaleName + "'  and uhcas.figi = '" + tradeObject.figi + "' and uhcas.candle_start_dt_utc > '" + tradeObject.openCandleDt + "'  order by uhcas.candle_start_dt_utc";

            List<string> candlesStrings = new PgExecuter(connectionString, log).ExecuteReader(getCandlesForAnalisys);

            return candlesStrings;
        }

    }
}