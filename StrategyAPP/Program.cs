using FinInvestLibrary.Functions.LocalOperations;
using FinInvestLibrary.Objects.StrategyObjects;
using FinInvestLibrary.Objects.Trade;
using log4net;

namespace StrategyAPP
{
    internal class Program
    {
        public static readonly ILog log = LogManager.GetLogger(typeof(Program));
        static string SESSION_ID = Guid.NewGuid().ToString();
        static string PAR1, PAR2, PAR3, PAR4, PAR5, PAR6, PAR7, PAR8, PAR9, CONNECTION_STRING;
        static int CANDLE_COUNT, TRIGGERS_COUNT, DATA_LESS_COUNT, TRIGGER_NOT_EXCLUDED, TRIGGER_NOT_ACTIVE_TRADE, TRADE_OBJECTS_COUNT, TRADE_ADDED_TO_REP, TRADE_ADDED_TO_COMM,
            ACTIVE_TRADE_COUNT, OPEN_CANDLES_COUNT, MATCHED_TRADE_CANDLES, TARGET_1_UPDATED, TARGET_1_COMMUNICATION, TARGET_2_UPDATED, TARGET_2_COMMUNICATION = 0;
        static void Main(string[] args)
        {

            string appPath = Environment.CurrentDirectory;
            string connectionStringPath = appPath + "\\connectionString.txt";
            CONNECTION_STRING = File.ReadAllText(connectionStringPath);

            log4net.Config.XmlConfigurator.Configure();

            var executer = new PgExecuter(CONNECTION_STRING, log);

            log.Info("/---------Session " + SESSION_ID + " is started...---------\\");
            log.Info(SESSION_ID + ": Проверяю конфигурацию");
            var confResult = GetAppConfiguration(args);

            if (confResult != null)
            {
                log.Info(SESSION_ID + ": Конфигурация успешно получена");

                if (confResult.APP_CONFIGURATION == "OPEN_TRADES")
                {
                    SearchNewTradeIdeas(executer, confResult);
                }
                else if (confResult.APP_CONFIGURATION == "CLOSE_TRADES")
                {
                    ControlOpenTradeIdeas(executer, confResult);
                }
            }
            else
            {
                log.Error(SESSION_ID + ": Не удалось получить конфигурацию. Проверьте входные параметры и повторите попытку");
            }

            log.Info("\\---------Session " + SESSION_ID + " finished---------/");
        }

        private static void ControlOpenTradeIdeas(PgExecuter executer, StrategyConfigurationObject confResult)
        {
            log.Info(SESSION_ID + ": Приложение запущено в режиме анализа существующих торговых идей");
            log.Info(SESSION_ID + ": Этап 1");
            log.Info(SESSION_ID + ": \tПолучаю список активных торговых идей");
            var trades = executer.GetActiveTrades(confResult.STRAT_NAME, confResult.STRAT_TYPE);
            ACTIVE_TRADE_COUNT = trades.Count;

            if (ACTIVE_TRADE_COUNT > 0)
            {
                log.Info(SESSION_ID + ": Этап 2");
                log.Info(SESSION_ID + ": \tПолучаю список открытых свечей");
                var candles = executer.GetOpenCandles("1_day_scale");
                OPEN_CANDLES_COUNT = candles.Count;

                if (OPEN_CANDLES_COUNT > 0)
                {
                    log.Info(SESSION_ID + ": Этап 3");
                    log.Info(SESSION_ID + ": \tПровожу сопоставление идей и открытых свечей");
                    List<TradeObject> candleTrades = new List<TradeObject>();
                    foreach (var trade in trades)
                    {
                        trade.candleObject = candles.FirstOrDefault(f => f.figi == trade.figi);
                        if (trade.candleObject != null)
                        {
                            candleTrades.Add(trade);
                            MATCHED_TRADE_CANDLES++;
                        }
                    }
                    if (MATCHED_TRADE_CANDLES > 0)
                    {
                        log.Info(SESSION_ID + ": Этап 4");
                        log.Info(SESSION_ID + ": \tАктуализирую статус торговых идей");

                        foreach (var trade in candleTrades)
                        {
                            log.Info(SESSION_ID + ": trade_id = " + trade.tradeId);

                            trade.communication_channel_id = confResult.STRAT_COMMUNICATION_CHANNEL_ID;


                            bool result_1_target = false;
                            bool result_2_target = false;
                            bool result_stopLoss = false;

                            float target1Price = 0;
                            float target2Price = 0;
                            float stopLossPrice = 0;


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

                            float maxPrice = (float)trade.candleObject.max_price;
                            float minPrice = (float)trade.candleObject.min_price;


                            if (trade.tradeType.Equals("LONG"))
                            {

                                if ((trade.candleObject.close_price >= target1Price || trade.candleObject.max_price >= target1Price) & !trade.target1CloseCause.Equals("PROFIT"))
                                {
                                    log.Info(SESSION_ID + ": \tTarget 1: (currClosePrice(" + trade.candleObject.close_price + ") >= target1Price(" + target1Price + ") или currMaxPrice(" + trade.candleObject.max_price + ") >= target1Price(" + target1Price + ")) и target1CloseCause != PROFIT (" + trade.target1CloseCause + ")");
                                    result_1_target = true;
                                    log.Info(SESSION_ID + ": \t\tРезультат вычисления = " + result_1_target);

                                }
                                if ((trade.candleObject.close_price >= target2Price || trade.candleObject.max_price >= target2Price) & !trade.target2CloseCause.Equals("PROFIT"))
                                {
                                    log.Info(SESSION_ID + ": \tTarget 2: (currClosePrice(" + trade.candleObject.close_price + ") >= target2Price(" + target2Price + ") или currMaxPrice(" + trade.candleObject.max_price + ") >= target2Price(" + target2Price + ")) и target2CloseCause != PROFIT (" + trade.target2CloseCause + ")");
                                    result_2_target = true;
                                    log.Info(SESSION_ID + ": \t\tРезультат вычисления = " + result_2_target);
                                }
                                if (trade.candleObject.close_price <= stopLossPrice || trade.candleObject.min_price <= stopLossPrice)
                                {
                                    log.Info(SESSION_ID + ": \tcurrClosePrice(" + trade.candleObject.close_price + ") <= stopLossPrice(" + stopLossPrice + ")");
                                    result_stopLoss = true;
                                    log.Info(SESSION_ID + ": \t\tРезультат вычисления = " + result_stopLoss);
                                }

                            }
                            else if (trade.tradeType.Equals("SHORT"))
                            {

                                if ((trade.candleObject.close_price <= target1Price || trade.candleObject.min_price <= target1Price) & !trade.target1CloseCause.Equals("PROFIT"))
                                {
                                    log.Info(SESSION_ID + ": \tTarget 1: (currClosePrice(" + trade.candleObject.close_price + ") <= target1Price(" + target1Price + ") или currMaxPrice(" + trade.candleObject.max_price + ") <= target1Price(" + target1Price + ")) и target1CloseCause != PROFIT (" + trade.target1CloseCause + ")");
                                    result_1_target = true;
                                    log.Info(SESSION_ID + ": \t\tРезультат вычисления = " + result_1_target);
                                }
                                if ((trade.candleObject.close_price <= target2Price || trade.candleObject.min_price <= target2Price) & !trade.target2CloseCause.Equals("PROFIT"))
                                {
                                    log.Info(SESSION_ID + ": \tTarget 2: (currClosePrice(" + trade.candleObject.close_price + ") <= target2Price(" + target2Price + ") или currMaxPrice(" + trade.candleObject.max_price + ") <= target2Price(" + target2Price + ")) и target2CloseCause != PROFIT (" + trade.target2CloseCause + ")");
                                    result_2_target = true;
                                    log.Info(SESSION_ID + ": \t\tРезультат вычисления = " + result_2_target);
                                }
                                if (trade.candleObject.close_price >= stopLossPrice || trade.candleObject.max_price >= stopLossPrice)
                                {
                                    log.Info(SESSION_ID + ": \tcurrClosePrice(" + trade.candleObject.close_price + ") >= stopLossPrice(" + stopLossPrice + ")");
                                    result_stopLoss = true;
                                    log.Info(SESSION_ID + ": \t\tРезультат вычисления = " + result_stopLoss);
                                }
                            }


                            //проверяем, что Target1 еще не закрыт (проверка по полю target1CloseCause)
                            if (!trade.target1CloseCause.Equals("PROFIT") & !trade.target1CloseCause.Equals("STOPLOSS_1"))
                            {
                                //Проверяем достижение первой цели
                                if (result_1_target)
                                {

                                    log.Debug("ПРОФИТ 1 достигнут!");
                                    //Обновляем информацию в сделке - достижение таргета и новый StopLoss
                                    trade.target1ClosePrice = target1Price;
                                    trade.target1CloseDT = DateTime.Now;
                                    trade.target1CloseCause = "PROFIT";
                                    trade.stopLoss2Value = float.Parse("0,001");



                                    trade.shareObject = executer.GetShare(trade.figi);
                                    trade.communication_template = "PROFIT_1_MACD_DONE.txt";
                                    string jsonObj = new PrepareData().JSONSerializedTrade(trade);

                                    bool isUpdatedTrade = executer.UpdateTarget1Trade(trade);
                                    if (isUpdatedTrade)
                                    {
                                        TARGET_1_UPDATED++;
                                        bool isUpdatedComm = executer.AddNewCommunication(jsonObj, trade);
                                        if (isUpdatedComm)
                                        {
                                            TARGET_1_COMMUNICATION++;
                                        }
                                    }

                                }

                                //Проверяем достижение второй цели. Если цель достигнута - закрываем рекомендацию
                                if (result_2_target)
                                {
                                    //ПРОФИТ 2 достигнут
                                    log.Info("ПРОФИТ 2 достигнут!");
                                    //Обновляем информацию в сделке - достижение таргета и новый StopLoss
                                    trade.target2ClosePrice = target2Price;
                                    trade.target2CloseDT = DateTime.Now;
                                    trade.target2CloseCause = "PROFIT";
                                    trade.trade_is_close_communication = true;

                                    trade.shareObject = executer.GetShare(trade.figi);
                                    trade.communication_template = "PROFIT_2_MACD_DONE.txt";
                                    string jsonObj = new PrepareData().JSONSerializedTrade(trade);

                                    bool isUpdatedTrade = executer.UpdateTarget2Trade(trade);
                                    if (isUpdatedTrade)
                                    {
                                        TARGET_2_UPDATED++;
                                        bool isUpdatedComm = executer.AddNewCommunication(jsonObj, trade);
                                        if (isUpdatedComm)
                                        {
                                            TARGET_2_COMMUNICATION++;
                                        }
                                    }
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
                                        trade.target1CloseDT = DateTime.Now;
                                        trade.target1CloseCause = "STOPLOSS_1";
                                        trade.trade_is_close_communication = true;


                                        trade.shareObject = executer.GetShare(trade.figi);
                                        trade.communication_template = "STOP_LOSS_MACD.txt";
                                        string jsonObj = new PrepareData().JSONSerializedTrade(trade);

                                        bool isUpdatedTrade = executer.UpdateStopLoss1Trade(trade);
                                        if (isUpdatedTrade)
                                        {
                                            bool isUpdatedComm = executer.AddNewCommunication(jsonObj, trade);
                                        }
                                        break;
                                    }
                                    //Сработал 2-й стоп: обновляем цифры и закрываем сделку
                                    else if (!trade.target2CloseCause.Equals("PROFIT"))
                                    {
                                        log.Info("Сработал второй стоп лосс");
                                        trade.target2ClosePrice = stopLossPrice;
                                        trade.target2CloseDT = DateTime.Now;
                                        trade.target2CloseCause = "STOPLOSS_2";
                                        trade.trade_is_close_communication = true;


                                        trade.shareObject = executer.GetShare(trade.figi);
                                        trade.communication_template = "STOP_LOSS_MACD.txt";
                                        string jsonObj = new PrepareData().JSONSerializedTrade(trade);

                                        bool isUpdatedTrade = executer.UpdateStopLoss2Trade(trade);
                                        if (isUpdatedTrade)
                                        {
                                            bool isUpdatedComm = executer.AddNewCommunication(jsonObj, trade);
                                        }
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
                                    //ПРОФИТ 2 достигнут
                                    log.Info("ПРОФИТ 2 достигнут!");
                                    //Обновляем информацию в сделке - достижение таргета и новый StopLoss
                                    trade.target2ClosePrice = target2Price;
                                    trade.target2CloseDT = DateTime.Now;
                                    trade.target2CloseCause = "PROFIT";
                                    trade.trade_is_close_communication = true;

                                    trade.shareObject = executer.GetShare(trade.figi);
                                    trade.communication_template = "PROFIT_2_MACD_DONE.txt";
                                    string jsonObj = new PrepareData().JSONSerializedTrade(trade);

                                    bool isUpdatedTrade = executer.UpdateTarget2Trade(trade);
                                    if (isUpdatedTrade)
                                    {
                                        TARGET_2_UPDATED++;
                                        bool isUpdatedComm = executer.AddNewCommunication(jsonObj, trade);
                                        if (isUpdatedComm)
                                        {
                                            TARGET_2_COMMUNICATION++;
                                        }
                                    }
                                    break;
                                }

                                //Проверяем на срабатывание StopLoss
                                if (result_stopLoss) //Решение о фиксации убытка принимается на основании цены закрытия сделки (в том числе и внутри дня)
                                {
                                    //Проверяем какой именно стоп сработал (от первой цели или от второй?)

                                    //Сработал 1-й стоп: обновляем цифры и закрываем сделку
                                    if (!trade.target1CloseCause.Equals("PROFIT"))
                                    {
                                        log.Info("Сработал первый стоп лосс");
                                        //Обновляем информацию в сделке - достижение таргета и новый StopLoss
                                        trade.target1ClosePrice = stopLossPrice;
                                        trade.target1CloseDT = DateTime.Now;
                                        trade.target1CloseCause = "STOPLOSS_1";
                                        trade.trade_is_close_communication = true;


                                        trade.shareObject = executer.GetShare(trade.figi);
                                        trade.communication_template = "STOP_LOSS_MACD.txt";
                                        string jsonObj = new PrepareData().JSONSerializedTrade(trade);

                                        bool isUpdatedTrade = executer.UpdateStopLoss1Trade(trade);
                                        if (isUpdatedTrade)
                                        {
                                            bool isUpdatedComm = executer.AddNewCommunication(jsonObj, trade);
                                        }
                                        break;
                                    }
                                    //Сработал 2-й стоп: обновляем цифры и закрываем сделку
                                    else if (!trade.target2CloseCause.Equals("PROFIT"))
                                    {
                                        log.Info("Сработал второй стоп лосс");
                                        trade.target2ClosePrice = stopLossPrice;
                                        trade.target2CloseDT = DateTime.Now;
                                        trade.target2CloseCause = "STOPLOSS_2";
                                        trade.trade_is_close_communication = true;


                                        trade.shareObject = executer.GetShare(trade.figi);
                                        trade.communication_template = "STOP_LOSS_MACD.txt";
                                        string jsonObj = new PrepareData().JSONSerializedTrade(trade);

                                        bool isUpdatedTrade = executer.UpdateStopLoss2Trade(trade);
                                        if (isUpdatedTrade)
                                        {
                                            bool isUpdatedComm = executer.AddNewCommunication(jsonObj, trade);
                                        }
                                        break;
                                    }

                                }
                            }

                            log.Info(SESSION_ID + ": currMaxPrice: " + trade.candleObject.max_price);
                            log.Info(SESSION_ID + ": currClosePrice: " + trade.candleObject.close_price);
                            log.Info(SESSION_ID + ": currMinPrice: " + trade.candleObject.min_price);
                            log.Info(SESSION_ID + ": Target 1 (" + target1Price + "): " + result_1_target);
                            log.Info(SESSION_ID + ": Target 2 (" + target2Price + "): " + result_2_target);
                            log.Info(SESSION_ID + ": StopLoss (" + stopLossPrice + "): " + result_stopLoss);

                            log.Info(SESSION_ID + ": ----------------------------->");




                        }
                    }

                }

            }

            log.Info(SESSION_ID + ": Результаты вычислений:");
            log.Info(SESSION_ID + ": \t\tАктивных идей: " + ACTIVE_TRADE_COUNT);
            log.Info(SESSION_ID + ": \t\tОткрытых свечей: " + OPEN_CANDLES_COUNT);
            log.Info(SESSION_ID + ": \t\tСопоставлено идей и свечей: " + MATCHED_TRADE_CANDLES);
        }

        private static void SearchNewTradeIdeas(PgExecuter executer, StrategyConfigurationObject confResult)
        {
            log.Info(SESSION_ID + ": Приложение запущено в режиме поиска новых торговых идей");
            log.Info(SESSION_ID + ": Заускаю стратегию " + confResult.STRAT_NAME);
            log.Info(SESSION_ID + ": Этап 1");
            log.Info(SESSION_ID + ": \tПолучаю актуальные свечи");
            var candles = executer.GetOpenCandles("1_day_scale");
            log.Info(SESSION_ID + ": Этап 2");
            var candlesCalculations = GetCalculationsForCandles(executer, confResult, candles);

            log.Info(SESSION_ID + ": Этап 3");
            log.Info(SESSION_ID + ": Запуск бизнес логики стратегии");
            List<FinInvestLibrary.Objects.Candle> triggerCandles = Strategy(confResult, candlesCalculations);
            if (triggerCandles.Count > 0)
            {
                log.Info(SESSION_ID + ": Этап 4");
                log.Info(SESSION_ID + ": Проверяю полученные триггеры на попадание в лист исключений");
                List<FinInvestLibrary.Objects.Candle> notExcludedCandles = CheckTriggerByExcluded(executer, triggerCandles);
                TRIGGER_NOT_EXCLUDED = notExcludedCandles.Count;

                if (notExcludedCandles.Count > 0)
                {
                    log.Info(SESSION_ID + ": Этап 5");
                    log.Info(SESSION_ID + ": Проверяю триггеры на наличие уже существующей торговой идеи в trades");
                    List<FinInvestLibrary.Objects.Candle> notActiveTrades = CheckTriggerByActiveTrades(executer, confResult, notExcludedCandles);
                    TRIGGER_NOT_ACTIVE_TRADE = notActiveTrades.Count;

                    if (notActiveTrades.Count > 0)
                    {
                        log.Info(SESSION_ID + ": Этап 6");
                        log.Info(SESSION_ID + ": Формирую объектные модели для сохранения и публикации идей");
                        List<TradeObject> tradeObjectList = CreateTradeObjects(executer, confResult, notActiveTrades);
                        TRADE_OBJECTS_COUNT = tradeObjectList.Count;

                        if (tradeObjectList.Count > 0)
                        {
                            log.Info(SESSION_ID + ": Этап 7");
                            log.Info(SESSION_ID + ": Сохраняю информацию о торговых идеях в репозиторий");
                            foreach (var tradeObject in tradeObjectList)
                            {
                                bool isAddTradeIdea = executer.AddNewTradeIdea(tradeObject);
                                if (isAddTradeIdea)
                                {
                                    TRADE_ADDED_TO_REP++;
                                    log.Info(SESSION_ID + ": Сохраняю информацию для отправки коммуникаций");
                                    var jsonObj = new PrepareData().JSONSerializedTrade(tradeObject);
                                    bool isAddedToComm = executer.AddNewCommunication(jsonObj, tradeObject);
                                    if (isAddedToComm) { TRADE_ADDED_TO_COMM++; }
                                }
                            }

                        }


                    }
                }
            }

            log.Info(SESSION_ID + ": Результаты вычислений:");
            log.Info(SESSION_ID + ": \t\tСвечей: " + CANDLE_COUNT);
            log.Info(SESSION_ID + ": \t\tТриггеров: " + TRIGGERS_COUNT);
            log.Info(SESSION_ID + ": \t\tНехватка данных: " + DATA_LESS_COUNT);
            log.Info(SESSION_ID + ": \t\tТриггеров в листе исключений: " + (TRIGGERS_COUNT - TRIGGER_NOT_EXCLUDED));
            log.Info(SESSION_ID + ": \t\tТриггеров c ранее открытыми идеями: " + (TRIGGER_NOT_EXCLUDED - TRIGGER_NOT_ACTIVE_TRADE));
            log.Info(SESSION_ID + ": \t\tСформировано идей: " + TRADE_OBJECTS_COUNT);
            log.Info(SESSION_ID + ": \t\tИдей сохранено в репозитории: " + TRADE_ADDED_TO_REP);
            log.Info(SESSION_ID + ": \t\tИдей передано на отправку: " + TRADE_ADDED_TO_COMM);
        }

        private static List<TradeObject> CreateTradeObjects(PgExecuter executer, StrategyConfigurationObject confResult, List<FinInvestLibrary.Objects.Candle> notActiveTrades)
        {
            List<TradeObject> tradeObjectList = new List<TradeObject>();
            foreach (var trigger in notActiveTrades)
            {
                try
                {
                    BorderPoints borderPoint = new BorderPoints();
                    borderPoint = executer.GetPersonalBorderPoints(trigger.figi, confResult.STRAT_NAME_FOR_BA, confResult.STRAT_DURATION_FOR_BA);

                    TradeObject tradeObject = new TradeObject();
                    tradeObject.shareObject = executer.GetShare(trigger.figi);
                    tradeObject.tradeId = Guid.NewGuid().ToString().Replace("-", "");
                    tradeObject.tradeType = confResult.STRAT_TYPE;
                    tradeObject.stratName = confResult.STRAT_NAME;
                    tradeObject.openCandleId = trigger.id;
                    tradeObject.openCandleDt = (DateTime)trigger.candle_start_dt;
                    tradeObject.figi = trigger.figi;
                    tradeObject.tradeStartDt = DateTime.Now;
                    tradeObject.openTradePrice = (float)trigger.close_price;
                    tradeObject.maxTradePrice = (float)trigger.max_price;
                    tradeObject.minTradePrice = (float)trigger.min_price;
                    tradeObject.maxtradepricecandleid = tradeObject.openCandleId;
                    tradeObject.maxtradepricecandledt = tradeObject.openCandleDt;
                    tradeObject.mintradepricecandleid = tradeObject.openCandleId;
                    tradeObject.mintradepricecandledt = tradeObject.openCandleDt;
                    tradeObject.target1Value = borderPoint.take_profit_point / 2;
                    tradeObject.target2Value = borderPoint.take_profit_point;
                    tradeObject.stopLoss1Value = borderPoint.stop_loss_point;
                    tradeObject.stopLoss2Value = borderPoint.stop_loss_point;
                    tradeObject.trade_is_close_analytic = false;
                    tradeObject.trade_is_close_communication = false;
                    tradeObject.target1CloseCause = "OPEN";
                    tradeObject.target2CloseCause = "OPEN";
                    tradeObject.communication_channel_id = confResult.STRAT_COMMUNICATION_CHANNEL_ID;
                    tradeObject.communication_template = confResult.STRAT_COMMUNICATION_TEMPLATE;
                    tradeObjectList.Add(tradeObject);
                }
                catch (Exception ex)
                {
                    log.Error(SESSION_ID + ": Ошибка формирования tradeObject");
                    log.Error(ex);
                }
            }

            return tradeObjectList;
        }

        private static List<FinInvestLibrary.Objects.Candle> CheckTriggerByActiveTrades(PgExecuter executer, StrategyConfigurationObject confResult, List<FinInvestLibrary.Objects.Candle> notExcludedCandles)
        {
            List<FinInvestLibrary.Objects.Candle> notActiveTrades = new List<FinInvestLibrary.Objects.Candle>();
            foreach (var candle in notExcludedCandles)
            {
                bool checkResult = executer.CheckTrades(candle.figi, confResult.STRAT_NAME, confResult.STRAT_TYPE);
                if (!checkResult)
                {
                    notActiveTrades.Add(candle);
                }
            }

            return notActiveTrades;
        }

        private static List<FinInvestLibrary.Objects.Candle> CheckTriggerByExcluded(PgExecuter executer, List<FinInvestLibrary.Objects.Candle> triggerCandles)
        {
            List<FinInvestLibrary.Objects.Candle> notExcludedCandles = new List<FinInvestLibrary.Objects.Candle>();
            foreach (var candle in triggerCandles)
            {
                bool checkResult = executer.CheckShareByExclude(candle.figi);
                if (!checkResult)
                {
                    notExcludedCandles.Add(candle);
                }
            }

            return notExcludedCandles;
        }

        private static List<FinInvestLibrary.Objects.Candle> Strategy(StrategyConfigurationObject confResult, List<FinInvestLibrary.Objects.Candle> candlesCalculations)
        {
            List<FinInvestLibrary.Objects.Candle> candles = null;
            if (confResult.STRAT_TYPE == "LONG")
            {
                candles = LongCalculation(confResult, candlesCalculations);

            }
            else
            {
                candles = ShortCalculation(confResult, candlesCalculations);
            }

            return candles;
        }

        private static List<FinInvestLibrary.Objects.Candle> ShortCalculation(StrategyConfigurationObject confResult, List<FinInvestLibrary.Objects.Candle> candlesCalculations)
        {
            List<FinInvestLibrary.Objects.Candle> candles = new List<FinInvestLibrary.Objects.Candle>();
            log.Info(SESSION_ID + ": Выбрана стратегия " + confResult.STRAT_TYPE);
            log.Info(SESSION_ID + ": Осуществляем поиск точки входа по следующему правилу: ");
            log.Info(SESSION_ID + ": \tPrevSLOW < PrevFAST && CurrentSLOW >= CurrentFAST");

            CANDLE_COUNT = candlesCalculations.Count;
            foreach (var candle in candlesCalculations)
            {
                //Проверка на nullRefException для всех вычислений;
                if (candle.calculation_current_slow is not null
                    && candle.calculation_current_fast is not null
                    && candle.calculation_prev_slow is not null
                    && candle.calculation_prev_fast is not null)
                {
                    //первое условие
                    bool prevCondition = candle.calculation_prev_slow.value < candle.calculation_prev_fast.value;
                    //второе условие
                    bool currCondition = candle.calculation_current_slow.value >= candle.calculation_current_fast.value;

                    if (prevCondition && currCondition)
                    {
                        log.Info(SESSION_ID + ": Сработало условие SHORT для " + candle.figi);
                        TRIGGERS_COUNT++;
                        candles.Add(candle);
                    }
                }
                else
                {
                    //log.Info(SESSION_ID + ": Вычисление точки входа невозможно. Одно или несколько вычислений не рассчитаны.");
                    DATA_LESS_COUNT++;
                }

            }
            log.Info(SESSION_ID + ": Результаты вычислений: Свечей: " + CANDLE_COUNT + " | Триггеров: " + TRIGGERS_COUNT + " | Нехватка данных: " + DATA_LESS_COUNT);
            return candles;
        }

        private static List<FinInvestLibrary.Objects.Candle> LongCalculation(StrategyConfigurationObject confResult, List<FinInvestLibrary.Objects.Candle> candlesCalculations)
        {
            List<FinInvestLibrary.Objects.Candle> candles = new List<FinInvestLibrary.Objects.Candle>();
            log.Info(SESSION_ID + ": Выбрана стратегия " + confResult.STRAT_TYPE);
            log.Info(SESSION_ID + ": Осуществляем поиск точки входа по следующему правилу: ");
            log.Info(SESSION_ID + ": \tPrevSLOW > PrevFAST && CurrentFAST >= CurrentSLOW ");


            CANDLE_COUNT = candlesCalculations.Count;
            foreach (var candle in candlesCalculations)
            {
                //Проверка на nullRefException для всех вычислений;
                if (candle.calculation_current_slow is not null
                    && candle.calculation_current_fast is not null
                    && candle.calculation_prev_slow is not null
                    && candle.calculation_prev_fast is not null)
                {
                    //первое условие
                    bool prevCondition = candle.calculation_prev_slow.value > candle.calculation_prev_fast.value;
                    //второе условие
                    bool currCondition = candle.calculation_current_fast.value >= candle.calculation_current_slow.value;

                    if (prevCondition && currCondition)
                    {
                        log.Info(SESSION_ID + ": Сработало условие LONG для " + candle.figi);
                        TRIGGERS_COUNT++;
                        candles.Add(candle);
                    }
                }
                else
                {
                    //log.Info(SESSION_ID + ": Вычисление точки входа невозможно. Одно или несколько вычислений не рассчитаны.");
                    DATA_LESS_COUNT++;
                }

            }
            log.Info(SESSION_ID + ": Результаты вычислений: Свечей: " + CANDLE_COUNT + " | Триггеров: " + TRIGGERS_COUNT + " | Нехватка данных: " + DATA_LESS_COUNT);
            return candles;
        }

        private static List<FinInvestLibrary.Objects.Candle> GetCalculationsForCandles(PgExecuter executer, StrategyConfigurationObject confResult, List<FinInvestLibrary.Objects.Candle> candles)
        {
            List<FinInvestLibrary.Objects.Candle> returnCandles = new List<FinInvestLibrary.Objects.Candle>();

            log.Info(SESSION_ID + ": \tПолучаю требуемые рассчеты из репозитория вычислений для каждой полученной свечи");
            foreach (var candle in candles)
            {
                log.Debug(SESSION_ID + ": Получаю SLOW вычисления за текущий день для: Candle_id " + candle.id + " - " + confResult.PAR1_TYPE + ":" + confResult.PAR1_DURATION);
                candle.calculation_current_slow = executer.GetCalculations(candle.id.ToString(), confResult.PAR1_TYPE, confResult.PAR1_DURATION, candle.scale);

                log.Debug(SESSION_ID + ": Получаю FAST вычисления за текущий день для: Candle_id " + candle.id + " - " + confResult.PAR2_TYPE + ":" + confResult.PAR2_DURATION);
                candle.calculation_current_fast = executer.GetCalculations(candle.id.ToString(), confResult.PAR2_TYPE, confResult.PAR2_DURATION, candle.scale);

                log.Debug(SESSION_ID + ": Получаю SLOW вычисления за предыдущий день для: Candle_id " + candle.id + " - " + confResult.PAR1_TYPE + ":" + confResult.PAR1_DURATION);
                candle.calculation_prev_slow = executer.GetPreviousCalculation(candle, confResult.PAR1_TYPE, confResult.PAR1_DURATION);

                log.Debug(SESSION_ID + ": Получаю FAST вычисления за предыдущий день для: Candle_id " + candle.id + " - " + confResult.PAR2_TYPE + ":" + confResult.PAR2_DURATION);
                candle.calculation_prev_fast = executer.GetPreviousCalculation(candle, confResult.PAR2_TYPE, confResult.PAR2_DURATION);
            }
            returnCandles = candles;

            return returnCandles;


        }

        /// <summary>
        /// работа с конфигурацией приложения
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        private static StrategyConfigurationObject GetAppConfiguration(string[] args)
        {
            StrategyConfigurationObject? returnValue = null;
            try
            {
#if DEBUG
                PAR1 = "91226/MACD_SL";
                PAR2 = "1226/MACD";
                PAR3 = "MACD(mid-term)";
                PAR4 = "LONG";
                PAR5 = "MACD";
                PAR6 = "1226";
                PAR7 = "-1001820470601";
                PAR8 = "OPEN_LONG_IDEA.txt";
                PAR9 = "OPEN_TRADES";
#else
                PAR1 = args[0];
                PAR2 = args[1];
                PAR3 = args[2];
                PAR4 = args[3];
                PAR5 = args[4];
                PAR6 = args[5];
                PAR7 = args[6];
                PAR8 = args[7];
                PAR9 = args[8];
#endif
                log.Debug(SESSION_ID + ": На входе получены параметры");
                log.Debug(SESSION_ID + ": PAR1 = " + PAR1);
                log.Debug(SESSION_ID + ": PAR2 = " + PAR2);
                log.Debug(SESSION_ID + ": PAR3 = " + PAR3);
                log.Debug(SESSION_ID + ": PAR4 = " + PAR4);
                log.Debug(SESSION_ID + ": PAR5 = " + PAR5);
                log.Debug(SESSION_ID + ": PAR6 = " + PAR6);
                log.Debug(SESSION_ID + ": PAR7 = " + PAR7);
                log.Debug(SESSION_ID + ": PAR8 = " + PAR8);
                log.Debug(SESSION_ID + ": PAR9 = " + PAR9);

                try
                {
                    returnValue = new StrategyConfigurationObject();
                    log.Debug(SESSION_ID + ": Приступаю к парсингу PAR1");
                    var splitPAR1 = PAR1.Split("/");
                    returnValue.PAR1_DURATION = splitPAR1[0];
                    returnValue.PAR1_TYPE = splitPAR1[1];

                    log.Debug(SESSION_ID + ": Приступаю к парсингу PAR2");
                    var splitPAR2 = PAR2.Split("/");
                    returnValue.PAR2_DURATION = splitPAR2[0];
                    returnValue.PAR2_TYPE = splitPAR2[1];

                    log.Debug(SESSION_ID + ": Получаю название стратегии");
                    returnValue.STRAT_NAME = PAR3;

                    log.Debug(SESSION_ID + ": Получаю тип стратегии");
                    returnValue.STRAT_TYPE = PAR4;

                    log.Debug(SESSION_ID + ": Навзвание стратегии для бизнес логики приложения");
                    returnValue.STRAT_NAME_FOR_BA = PAR5;

                    log.Debug(SESSION_ID + ": Получаю длительность для бизнес логики приложения");
                    returnValue.STRAT_DURATION_FOR_BA = PAR6;

                    log.Debug(SESSION_ID + ": Получаю идентификатор канала отправки");
                    returnValue.STRAT_COMMUNICATION_CHANNEL_ID = PAR7;

                    log.Debug(SESSION_ID + ": Получаю идентификатор канала отправки");
                    returnValue.STRAT_COMMUNICATION_TEMPLATE = PAR8;

                    log.Debug(SESSION_ID + ": Получаю идентификатор канала отправки");
                    returnValue.APP_CONFIGURATION = PAR9;

                    if (returnValue.STRAT_TYPE != "LONG" && returnValue.STRAT_TYPE != "SHORT")
                    {
                        log.Error(SESSION_ID + ": Указанный в конфигурации тип стратегии не является допустимым. Допустимые типы: LONG/SHORT");
                        returnValue = null;
                    }


                }
                catch (Exception ex)
                {
                    log.Error(SESSION_ID + ": возникла ошибка");
                    log.Error(SESSION_ID + ": " + ex.ToString());
                    returnValue = null;
                }
            }
            catch (Exception ex)
            {
                log.Error(SESSION_ID + ": возникла ошибка");
                log.Error(SESSION_ID + ": " + ex.ToString());

                returnValue = null;
            }

            return returnValue;

        }
    }
}