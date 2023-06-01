using FinInvestLibrary.Objects;
using log4net;
using FinInvestLibrary.Functions.LocalOperations;
using FinInvestLibrary.Functions.Mathematica;
using Microsoft.VisualBasic;
using Tinkoff.InvestApi.V1;


namespace TA.Metrics.Calculater
{
    internal class Program
    {
        public static readonly ILog log = LogManager.GetLogger(typeof(Program));
        
        static void Main(string[] args)
        {
            string duration, connectionString, scale, appConfiguration, calcType;
            log4net.Config.XmlConfigurator.Configure();

#if DEBUG
            appConfiguration = "RegularCalculate";
            duration = "7,12,18,26,50,200";
            scale = "1_day_scale";
            calcType = "EMA";
#else
            duration = args[0];
            scale = args[1];
            appConfiguration = args[2];
            calcType = args[3];
#endif
            string appPath = Environment.CurrentDirectory;
            string connectionStringPath = appPath + "\\connectionString.txt";
            connectionString = File.ReadAllText(connectionStringPath);

            List<string> durationsList = duration.Split(',').ToList();
            List<string> scalesList = scale.Split(',').ToList();

            log.Info("Текущая конфигурация:");
            log.Info("\tДлительности для расчета:");
            foreach (var str in durationsList)
            { log.Info("\t\t" + str); }
            log.Info("\tМасштабы для расчета:");
            foreach (var str in scalesList)
            { log.Info("\t\t" + str); }

            log.Info("Приложение запущено в режиме " + appConfiguration);
            log.Info("Расчет производится в режиме " + calcType);

            if (calcType == "MA")
            {
                MACalc(connectionString, scale, appConfiguration, calcType, durationsList);
            }
            else if (calcType == "EMA")
            {
                EMACalc(connectionString, scale, appConfiguration, calcType, durationsList);
            }
        }

        private static void EMACalc(string connectionString, string scale, string appConfiguration, string calcType, List<string> durationsList)
        {
            var sharesList = new PgExecuter(connectionString, log).GetActualSharesList();
            log.Info("Список инструментов для расчета содержит " + sharesList.Count + " позиций");

            if (appConfiguration == "HistoryCalculate")
            {
                CalculateEMAForAllNotEMACandles(connectionString, scale, calcType, durationsList, sharesList);
            }
            else if (appConfiguration == "RegularCalculate")
            {
                CalculateEMAForOpenCandles(connectionString, scale, calcType, durationsList, sharesList);
            }
        }

        private static void CalculateEMAForOpenCandles(string connectionString, string scale,  string calcType, List<string> durationsList, List<ShareObject> sharesList)
        { 
            foreach(var share in sharesList) 
            {
                log.Info("Ищу ОТКРЫТЫЕ свечи без расчета по " + calcType + " для актива " + share.name + "(" + share.figi + ")");

                foreach (var dur in durationsList)
                {
                    log.Info("\tПроверяю длительность: " + dur);
                    var candles = new PgExecuter(connectionString, log).GetOpenCandles(scale, share);
                    log.Info("\t\tНайдено " + candles.Count + " свечей");

                    log.Info("Расчитываю EMA по активу " + share.name + "(" + share.figi + "), длительность интервала расчета = " + dur);
                    foreach (var candle in candles)
                    {
                        log.Info("Для свечи candle_id = " + candle.id + " ищу предыдущее значение EMA");
                        float prevEMAValue = new PgExecuter(connectionString, log).GetPreviousValue(candle, dur, calcType);
                        if (prevEMAValue == -1)
                        {
                            log.Info("Для свечи candle_id = " + candle.id + " ищу предыдущее значение EMA не найдено.");
                            log.Info("Запустите TA.Metrics.Calculater в режиме \"HistoryCalculate\" или проверьте хранилище свечей");
                        }
                        else
                        {
                            //рассчитываем EMA с применением MA (для первого прохода)
                            float alphaValue = new MathEMA().getAlphaValueForDuration(Convert.ToInt32(dur));
                            log.Info("Рассчитана alpha = " + alphaValue);
                            float curEMAValue = new MathEMA().getEMAValue(alphaValue, candle, prevEMAValue);
                            log.Info("Рассчитана EMA = " + curEMAValue);
                            log.Info("Вставляю новую строку в calculations");

                            log.Info("Проверяю наличие предудущего расчета в таблице calculations  для свечи ID = " + candle.id);
                            int isExistMACalcForCandle = new PgExecuter(connectionString, log).CheckMACalculationForOpenCandle(candle, calcType, Convert.ToInt32(dur));
                            if (isExistMACalcForCandle > 0)
                            {
                                log.Info("Предыдущий расчет существует. Он будет обновлен на новое значение");
                                bool isUpdatetd = new PgExecuter(connectionString, log).UpdateCalculationsTable(candle, calcType, Convert.ToInt32(dur), curEMAValue.ToString());
                                if (isUpdatetd)
                                    log.Info("Успешно обновлено значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                                else
                                    log.Info("Не удалось обновить значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                            }
                            else
                            {
                                log.Info("Предыдущий расчет НЕ существует.Записываю результат по MA в таблицу calculations");
                                bool isInserted =new PgExecuter(connectionString, log).InsertIntoCalculationsTable(candle, calcType, Convert.ToInt32(dur), curEMAValue.ToString());
                                if (isInserted)
                                    log.Info("Успешно встравлено значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                                else
                                    log.Info("Не удалось вставить значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                            }

                            
                            
                        }
                    }
                }
            }
        }

        private static void CalculateEMAForAllNotEMACandles(string connectionString, string scale, string calcType, List<string> durationsList, List<ShareObject> sharesList)
        {
            //Первичный расчет
            //Получаем набор свечей без расчета по EMA
            foreach (var share in sharesList)
            {
                log.Info("Ищу ЗАКРЫТЫЕ свечи без расчета по " + calcType + " для актива " + share.name + "(" + share.figi + ")");
                foreach (var dur in durationsList)
                {
                    log.Info("Обрабатываю длительность " + dur);
                    var candles = new PgExecuter(connectionString, log).GetCandlesWithoutCalculation(calcType, scale, share, dur);
                    candles = candles.OrderBy(o => o.candle_start_dt).ToList();

                    //Проверяем наличие предыдущего значения EMA для каждой свечи
                    foreach (var candle in candles)
                    {
                        log.Info("Для свечи candle_id = " + candle.id + " ищу предыдущее значение EMA");
                        float prevEMAValue = new PgExecuter(connectionString, log).GetPreviousValue(candle, dur, calcType);
                        if (prevEMAValue == -1)
                        {
                            log.Info("Для свечи candle_id = " + candle.id + " предыдущее значение EMA не найдено.");
                            log.Info("Для свечи candle_id = " + candle.id + " ищу предыдущее значение MA");
                            float prevMAValue = new PgExecuter(connectionString, log).GetPreviousValue(candle, dur, "MA");
                            if (prevMAValue == -1)
                            {
                                log.Info("Для свечи candle_id = " + candle.id + " не найдены предыдущие значения EMA и MA. Перехожу к следующей свече");
                            }
                            else
                            {
                                log.Info("Для свечи candle_id = " + candle.id + " найдено предыдуще значение MA = " + prevMAValue);

                                log.Info("Приступаю к расчету EMA для свечи candle_id = " + candle.id);

                                //рассчитываем EMA с применением MA (для первого прохода)
                                float alphaValue = new MathEMA().getAlphaValueForDuration(Convert.ToInt32(dur));
                                log.Info("Рассчитана alpha = " + alphaValue);
                                float curEMAValue = new MathEMA().getEMAValue(alphaValue, candle, prevMAValue);
                                log.Info("Рассчитана EMA = " + curEMAValue);
                                log.Info("Вставляю новую строку в calculations");
                                bool isInserted = new PgExecuter(connectionString, log).InsertIntoCalculationsTable(candle, calcType, Convert.ToInt32(dur), curEMAValue.ToString());
                                if (isInserted)
                                    log.Info("Успешно встравлено значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                                else
                                    log.Info("Не удалось вставить значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                            }
                        }
                        else
                        {
                            log.Info("Для свечи candle_id = " + candle.id + " найдено предыдуще значение EMA = " + prevEMAValue);

                            log.Info("Приступаю к расчету EMA для свечи candle_id = " + candle.id);

                            //рассчитываем EMA с применением MA (для первого прохода)
                            float alphaValue = new MathEMA().getAlphaValueForDuration(Convert.ToInt32(dur));
                            log.Info("Рассчитана alpha = " + alphaValue);
                            float curEMAValue = new MathEMA().getEMAValue(alphaValue, candle, prevEMAValue);
                            log.Info("Рассчитана EMA = " + curEMAValue);

                            bool isInserted = new PgExecuter(connectionString, log).InsertIntoCalculationsTable(candle, calcType, Convert.ToInt32(dur), curEMAValue.ToString());
                            if (isInserted)
                                log.Info("Успешно встравлено значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                            else
                                log.Info("Не удалось вставить значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                        }
                    }
                }
            }
        }

        private static void MACalc(string connectionString, string scale, string appConfiguration, string calcType, List<string> durationsList)
        {
            //Расчет MA
            //Получаем список инструментов, по которым требуется произвести расчеты.
            var sharesList = new PgExecuter(connectionString, log).GetActualSharesList();
            log.Info("Список инструментов для расчета содержит " + sharesList.Count + " позиций");

            if (appConfiguration == "HistoryCalculate")
            {
                CalculateMAForAllNotMACAndles(connectionString, scale, durationsList, calcType, sharesList);
            }
            else if (appConfiguration == "RegularCalculate")
            {
                CalculateMAForOpenCandles(connectionString, scale, durationsList, calcType, sharesList);
            }
        }

        private static void CalculateMAForOpenCandles(string connectionString, string scale, List<string> durationsList, string calcType, List<ShareObject> sharesList)
        {
            foreach (var share in sharesList)
            {
                log.Info("Ищу ОТКРЫТЫЕ свечи без расчета по " + calcType + " для актива " + share.name + "(" + share.figi + ")");

                foreach (var dur in durationsList)
                {
                    log.Info("\tПроверяю длительность: " + dur);
                    var candles = new PgExecuter(connectionString, log).GetOpenCandles(scale, share);
                    log.Info("\t\tНайдено " + candles.Count + " свечей");

                    log.Info("Расчитываю MA по активу " + share.name + "(" + share.figi + "), длительность интервала расчета = " + dur);
                    foreach (var candle in candles)
                    {
                        new MathMA(connectionString, log).MACalcOpenCandles(candle, dur, calcType);
                    }
                }
            }
        }

        private static void CalculateMAForAllNotMACAndles(string connectionString, string scale, List<string> durationsList, string calcType, List<ShareObject> sharesList)
        {
            //Для каждого инструмента и длительности, а также масштабу находим все свечи, по которым НЕ рассчитана метрика MA 
            //Только закрытые свечи
            foreach (var share in sharesList)
            {

                //Первичный расчет
                log.Info("Ищу ЗАКРЫТЫЕ свечи без расчета по " + calcType + " для актива " + share.name + "(" + share.figi + ")");


                foreach (var dur in durationsList)
                {
                    log.Info("\tПроверяю длительность: " + dur);

                    var candles = new PgExecuter(connectionString, log).GetCandlesWithoutCalculation(calcType, scale, share, dur);
                    log.Info("\t\tНайдено " + candles.Count + " свечей");
                    candles = candles.OrderBy(o => o.candle_start_dt).ToList();

                    //Производим расчет MA
                    log.Info("Расчитываю MA по активу " + share.name + "(" + share.figi + "), длительность интервала расчета = " + dur);
                    foreach (var candle in candles)
                    {
                        new MathMA(connectionString, log).MACalcAll(candle, dur, calcType);
                    }
                }
            }
        }
    }
}