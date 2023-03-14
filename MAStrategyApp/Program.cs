using log4net;
using FinInvestLibrary.Functions;
using FinInvestLibrary.Functions.LocalOperations;
using FinInvestLibrary.Objects.Trade;
using Tinkoff.InvestApi.V1;
using FinInvestLibrary.Objects;
using System.Net.WebSockets;
using System.Globalization;
using Npgsql.Replication.PgOutput.Messages;

namespace MAStrategyApp

{
    internal class Program
    {
        public static readonly ILog log = LogManager.GetLogger(typeof(Program));
        static int Main(string[] args)
        {

            string runType = args[0];
            string strategyName = "MA_12/26";// args[1];
            string scaleName = "1_day_scale";//args[2];
            int exitCode = 9999;
            log4net.Config.XmlConfigurator.Configure();
            log.Info(@"/---------Start---------\");
            log.Info("Зачитываю конфигурацию");
            DateTime currentDateTime = DateTime.UtcNow; //Tinkoff API работает всегда в UTC - придерживаемся тоже UTC;
            string appPath = Environment.CurrentDirectory;
            string connectionStringPath = appPath + "\\connectionString.txt";
            string connectionString = File.ReadAllText(connectionStringPath);
            log.Info("Строка подключения: " + connectionString);

            var shares = new FinBaseConnector().GetSharesFromDB(connectionString);
            //Вызов стратегии
            if (runType.Equals("trade_open_point"))
            {
                MA_Strategy_OpenTrade(strategyName, scaleName, connectionString, shares);
            }
            else if (runType.Equals("trade_close_point"))
            {
                MA_Strategy_TradeWorker(connectionString, scaleName);
            }


            // 1) добавить сохранения максимального candle id - Done
            // 2) разраюотать сценарий мониторинга сделки
            // 3) разработать сценарий закрытия сделки



            return exitCode;
        }

        private static void MA_Strategy_TradeWorker(string connectionString, string scaleName)
        {
            //получаем список активных сделок
            List<TradeObject> tradeObjectList = GetActiveTrades(connectionString);

            //Обрабатываю каждую сделку и ищу экстремумы + точки выхода
            foreach (var trade in tradeObjectList)
            {
                DateTime executeStartDT = DateTime.UtcNow;
                //для каждой сделки нужно найти все последующие свечи
                //получаю список строк для конвертации их в объект candle
                List<string> candlesStringList = GetCandlesSMARows(scaleName, connectionString, trade);
                List<CandleForSMAStratAnalysis> candleForSMAStratAnalyses = GetCandlesSMAObjects(candlesStringList);

                //каждую полученную свечу проверяем на удовлетворение условию стратегии (пересечение короткой линией вниз длинную линию)
                for(int i = 1; i < candleForSMAStratAnalyses.Count; i++)
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


                    if (trade.tradeType.Equals("LONG")) //Проверяем на LONG условия короткая должна пересечь длинную сверху вниз
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
                                trade.tradeProfitByClose = trade.closeTradePrice - trade.openTradePrice;
                                trade.tradeProfitByClosePerc = trade.tradeProfitByClose / trade.openTradePrice;
                                trade.tradeProfitByMax = trade.maxTradePrice - trade.openTradePrice;
                                trade.tradeProfitByMaxPerc = trade.tradeProfitByMax / trade.openTradePrice;
                                trade.tradeProfitByMin = trade.minTradePrice - trade.openTradePrice;
                                trade.tradeProfitByMinPerc = trade.tradeProfitByMin / trade.openTradePrice;
                                trade.tradeDuration = trade.closeCandleDt - trade.openCandleDt;

                                CloseTrade(trade, connectionString); //Закрываем сделку

                                log.Info("Сделка " + trade.tradeType + " ID:" + trade.tradeId + " закрыта");
                                break;
                            }
                        }
                    }
                    else if (trade.tradeType.Equals("SHORT")) //Проверяем на SHORT условия - короткая должна пересечь длинную снизу вверх
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
                                trade.tradeProfitByClose = (trade.closeTradePrice - trade.openTradePrice) * -1;
                                trade.tradeProfitByClosePerc = (trade.tradeProfitByClose / trade.openTradePrice);
                                trade.tradeProfitByMax = trade.openTradePrice - trade.maxTradePrice;
                                trade.tradeProfitByMaxPerc = trade.tradeProfitByMax / trade.openTradePrice;
                                trade.tradeProfitByMin = trade.minTradePrice - trade.openTradePrice;
                                trade.tradeProfitByMinPerc = trade.tradeProfitByMin / trade.openTradePrice;
                                trade.tradeDuration = trade.closeCandleDt - trade.openCandleDt;
                                log.Info("Сделка " + trade.tradeType + " ID:" + trade.tradeId + " закрыта");
                                CloseTrade(trade, connectionString); //Закрываем сделку
                                break;
                            }
                        }
                    }
                            
                }

                

                DateTime executeFinishDT = DateTime.UtcNow;
                log.Debug("Время выполнения: " + (executeFinishDT - executeStartDT).TotalSeconds + " секунд.");

            }

        }

        private static void UpdateTrade(TradeObject trade, string connectionString)
        {
            string sqlCommand = "update public.trades t set maxTradePrice = '" + trade.maxTradePrice.ToString().Replace(',','.') 
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
                + "', tradeProfitByClose = '" + trade.tradeProfitByClose.ToString().Replace(',', '.')
                + "', tradeProfitByClosePerc = '" + trade.tradeProfitByClosePerc.ToString().Replace(',', '.')
                + "', tradeProfitByMax = '" + trade.tradeProfitByMax.ToString().Replace(',','.') 
                + "', tradeProfitByMaxPerc = '" + trade.tradeProfitByMaxPerc.ToString().Replace(',','.') 
                + "', tradeProfitByMin = '" + trade.tradeProfitByMin.ToString().Replace(',','.') 
                + "', tradeProfitByMinPerc = '" + trade.tradeProfitByMinPerc.ToString().Replace(',','.') 
                + "', tradedurationsec = " + trade.tradeDuration.TotalSeconds 
                + " where t.tradeid = '" + trade.tradeId + "'";
            new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand);
        }

        private static List<TradeObject> GetActiveTrades(string connectionString)
        {
            log.Info("Получаю список активных сделок");
            string sqlCommand = "select tradeId,tradeType,stratName,openCandleId,openCandleDt,figi,tradeStartDt,openTradePrice, maxTradePrice, minTradePrice from trades where closecandleid is null";
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

                tradeObjectList.Add(tradeObject);
            }
            log.Info("Найдено " + tradeObjectList.Count + " активных сделок");
            return tradeObjectList;
        }

        private static void MA_Strategy_OpenTrade(string strategyName, string scaleName, string connectionString, List<ShareObject> shares)
        {
            var sharesForStrategy = shares.Where(w => w.country_of_risk.Equals("RU"));
            Console.WriteLine(sharesForStrategy.ToList().Count());
            GetFigiLastCandleId(strategyName, scaleName, connectionString, sharesForStrategy.ToList());
            GetCandlesSMARows(scaleName, connectionString, sharesForStrategy.ToList()); //в коллекцию shares (в каждый элемент) добавляем колелкцию свечей для проведения анализа по стратегии MA

            //Анализируем на предмет пересечения быстрой и медленной линии
            log.Info("Запуск стратегии");
            foreach (var share in sharesForStrategy)
            {
                List<TradeObject> tradeObjectList = new List<TradeObject>();
                //Проверяем каждую свечу на предмет пробоя короткой линией длинной линии. При этом учитываем значение предыдущей свечи. Если короткая
                for (int i = 1; i < share.candleForSMAStratAnalysisList.Count; i++)
                {

                    var prevValue = share.candleForSMAStratAnalysisList[i - 1].fastInterval - share.candleForSMAStratAnalysisList[i - 1].slowInterval;
                    var currValue = share.candleForSMAStratAnalysisList[i].fastInterval - share.candleForSMAStratAnalysisList[i].slowInterval;



                    //Поиск точки входа в сделку LONG
                    if (prevValue < 0)
                    {
                        if (currValue > 0)
                        {
                            TradeObject tradeObject = new TradeObject();
                            tradeObject.tradeId = Guid.NewGuid().ToString();
                            tradeObject.tradeType = "LONG";
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

                            tradeObjectList.Add(tradeObject);
                            log.Info("LONG: " + share.ticker + ", CandleID: " + tradeObject.openCandleId + ", Date: " + tradeObject.openCandleDt);
                        }
                    }
                    //Поиск точки входа в сделку SHORT
                    if (prevValue >= 0)
                    {
                        if (currValue < 0)
                        {
                            TradeObject tradeObject = new TradeObject();
                            tradeObject.tradeId = Guid.NewGuid().ToString();
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

                            tradeObjectList.Add(tradeObject);
                            log.Info("SHORT: " + share.ticker + ", CandleID: " + tradeObject.openCandleId + ", Date: " + tradeObject.openCandleDt);
                        }
                    }
                }
                share.tradeObjects = tradeObjectList;
            }

            //Сохраняем найденные сделки в таблицу

            var sharesFor = sharesForStrategy.ToList();
            log.Info("Сохраняю найденные сделки по стратегии: " + strategyName);
            int tradesCount = 0;
            for (int i = 0; i < sharesFor.Count(); i++)
            {
                log.Info("Сохраняю сделки по активу: " + sharesFor[i].name + "(" + sharesFor[i].figi + ")");
                for (int ii = 0; ii < sharesFor[i].tradeObjects.Count; ii++)
                {
                    string sqlCommand = "INSERT INTO public.trades (tradeId,tradeType,stratName,openCandleId,openCandleDt,figi,tradeStartDt,openTradePrice,maxTradePrice,minTradePrice,maxtradepricecandleid,maxtradepricecandledt,mintradepricecandleid,mintradepricecandledt) VALUES('" 
                        + sharesFor[i].tradeObjects[ii].tradeId + "','" 
                        + sharesFor[i].tradeObjects[ii].tradeType + "','" 
                        + sharesFor[i].tradeObjects[ii].stratName + "','" 
                        + sharesFor[i].tradeObjects[ii].openCandleId + "','" 
                        + sharesFor[i].tradeObjects[ii].openCandleDt + "','" 
                        + sharesFor[i].tradeObjects[ii].figi + "','" 
                        + sharesFor[i].tradeObjects[ii].tradeStartDt + "','" 
                        + sharesFor[i].tradeObjects[ii].openTradePrice.ToString().Replace(',', '.') + "','" 
                        + sharesFor[i].tradeObjects[ii].maxTradePrice.ToString().Replace(',', '.') + "','"
                        + sharesFor[i].tradeObjects[ii].minTradePrice.ToString().Replace(',', '.') +"'," 
                        + sharesFor[i].tradeObjects[ii].maxtradepricecandleid + ",'" 
                        + sharesFor[i].tradeObjects[ii].maxtradepricecandledt + "'," 
                        + sharesFor[i].tradeObjects[ii].mintradepricecandleid +",'" 
                        + sharesFor[i].tradeObjects[ii].mintradepricecandledt + "')";
                    log.Debug("Команда для сохранения сделки: ");
                    log.Debug(sqlCommand);
                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommand);
                    tradesCount++;


                }
                if (sharesFor[i].candleForSMAStratAnalysisList.Count > 0)
                {
                    log.Info("Сохранено " + tradesCount + " сделок");
                    log.Info("Обновляю максимальный candle_id по активу " + sharesFor[i].name + "(" + sharesFor[i].figi + ")");

                    string sqlCommandUpd = "update cfg_last_candles_for_strategy t set candle_id = '" + sharesFor[i].candleForSMAStratAnalysisList.Max(m => m.candleId) + "', update_dt = now() where t.figi = '" + sharesFor[i].figi + "' and strategy_name = '" + strategyName + "' and calc_scale = '" + scaleName + "'";
                    log.Debug("Команда для обновления максимального candleId: ");
                    log.Debug(sqlCommandUpd);
                    new PgExecuter(connectionString, log).ExecuteNonQuery(sqlCommandUpd);
                }



            }
            log.Info("Завершен процесс поиска входа в сделки");
        }

        private static void GetFigiLastCandleId(string strategyName, string scaleName, string connectionString, List<ShareObject> shares)
        {
            string getLastCandleIdComm = string.Empty;
            for (int i = 0; i < shares.Count;i++) 
            {
                getLastCandleIdComm = "select candle_id from public.cfg_last_candles_for_strategy where strategy_name = '" + strategyName + "' and calc_scale = '" + scaleName + "' and figi = '" + shares[i].figi + "'";
                log.Info(getLastCandleIdComm);
                var res = new PgExecuter(connectionString, log).ExecuteScalarQuery(getLastCandleIdComm);
                shares[i].LastCandleIdForStrategy = Convert.ToInt32(res);
            }
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
                getCandlesForAnalisys = "select id, figi, candle_start_dt_utc, interval_12, interval_26, open_price, close_price ,min_price, max_price  from public.union_history_candles_all_scales uhcas join union_candles_all_intervals ucai on uhcas.id = ucai.candle_id where ucai.calculate_type = 'MOVING_AVG_CLOSE'  and uhcas.scale = '" + scaleName + "'  and uhcas.figi = '" + shares[i].figi + "' and uhcas.id > " + shares[i].LastCandleIdForStrategy + " order by uhcas.candle_start_dt_utc";

                List<string> shareCandlesFoAnalysisStrings = new PgExecuter(connectionString, log).ExecuteReader(getCandlesForAnalisys);

                shares[i].candleForSMAStratAnalysisList = GetCandlesSMAObjects(shareCandlesFoAnalysisStrings);
                log.Info("Осталось обработать: " + (shares.Count - (i + 1)).ToString());
            }
        }

        private static List<string> GetCandlesSMARows(string scaleName, string connectionString, TradeObject tradeObject)
        {

            string getCandlesForAnalisys = string.Empty;
            
            getCandlesForAnalisys = "select id, figi, candle_start_dt_utc, interval_12, interval_26, open_price, close_price ,min_price, max_price  from public.union_history_candles_all_scales uhcas join union_candles_all_intervals ucai on uhcas.id = ucai.candle_id where ucai.calculate_type = 'MOVING_AVG_CLOSE'  and uhcas.scale = '" + scaleName + "'  and uhcas.figi = '" + tradeObject.figi + "' and uhcas.id >= " + tradeObject.openCandleId + " order by uhcas.candle_start_dt_utc";

            List<string> candlesStrings = new PgExecuter(connectionString, log).ExecuteReader(getCandlesForAnalisys);

            return candlesStrings;
        }
    }
}