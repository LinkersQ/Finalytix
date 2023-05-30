using FinInvestLibrary.Objects;
using log4net;
using FinInvestLibrary.Functions.LocalOperations;
using Microsoft.VisualBasic;
using Tinkoff.InvestApi.V1;

namespace TA.Metrics.Calculater
{
    internal class Program
    {
        public static readonly ILog log = LogManager.GetLogger(typeof(Program));
        
        static void Main(string[] args)
        {
            string duration, connectionString, scale, appConfiguration;
            log4net.Config.XmlConfigurator.Configure();

#if DEBUG
            appConfiguration = "RegularCalculate";
            duration = "7,12,18,26,50,200";
            scale = "1_day_scale";
#else
            duration = args[0];
            scale = args[1];
            appConfiguration = args[2];
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

            //Расчет MA
            string calcType = "MA";
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
                        new Math(connectionString, log).MACalcOpenCandles(candle, dur, calcType);
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
                        new Math(connectionString, log).MACalcAll(candle, dur, calcType);
                    }
                }
            }
        }
    }
}