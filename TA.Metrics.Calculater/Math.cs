using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FinInvestLibrary.Objects;
using log4net;
using FinInvestLibrary.Functions.LocalOperations;
using Npgsql;

namespace TA.Metrics.Calculater
{
    public class Math
    {
        private string connectionString = string.Empty;
        private NpgsqlConnection connection = null;
        private ILog log = null;
        /// <summary>
        /// Инцициализация класса
        /// </summary>
        /// <param name="connString">строка подключения к бд</param>
        /// <param name="log">объект логгера для низкоуровнего логирования</param>
        public Math(string connString, ILog inputlog)
        {
            connectionString = connString;
            connection = new NpgsqlConnection(connString);
            log = inputlog;
        }


        public void MACalcAll(Candle candle, string duration, string calcType)
        {
            int dur = Convert.ToInt32(duration);

            //запрашиваем набор свечей для дальнейшего расчета
            //определяем граничные даты (в запросе к БД выкидываются выходные дни)
            log.Info("Получаю свечи для расчета по свече с ID = " + candle.id + "(" + candle.candle_start_dt + ")");
            var candlesForCalc = new PgExecuter(connectionString, log).GetCandlesForCalc(candle, duration);
            log.Info("Получено " + candlesForCalc.Count + " свечей");

            if (candlesForCalc.Count == dur)
            {
                log.Info("Рассчитываю MA " + dur + " для свечи ID = " + candle.id + "(" + candle.candle_start_dt + ")");
                var sumClosedPrice = candlesForCalc.Sum(s => s.close_price);
                var ma = sumClosedPrice / dur;
                log.Info("MA " + dur + " для свечи ID = " + candle.id + "(" + candle.candle_start_dt + ") = " + ma);

                log.Info("Записываю результат по MA в таблицу calculations");
                new PgExecuter(connectionString, log).InsertIntoCalculationsTable(candle, calcType, dur, ma.ToString());
            }
        }

        public void MACalcOpenCandles(Candle candle, string duration, string calcType)
        {
            int dur = Convert.ToInt32(duration);

            //запрашиваем набор свечей для дальнейшего расчета
            //определяем граничные даты (в запросе к БД выкидываются выходные дни)
            log.Info("Получаю свечи для расчета по свече с ID = " + candle.id + "(" + candle.candle_start_dt + ")");
            var candlesForCalc = new PgExecuter(connectionString, log).GetCandlesForCalc(candle, duration);
            log.Info("Получено " + candlesForCalc.Count + " свечей");

            if (candlesForCalc.Count == dur)
            {
                log.Info("Рассчитываю MA " + dur + " для свечи ID = " + candle.id + "(" + candle.candle_start_dt + ")");
                var sumClosedPrice = candlesForCalc.Sum(s => s.close_price);
                var ma = sumClosedPrice / dur;
                log.Info("MA " + dur + " для свечи ID = " + candle.id + "(" + candle.candle_start_dt + ") = " + ma);

                log.Info("Проверяю наличие предудущего расчета в таблице calculations  для свечи ID = " + candle.id);
                int isExistMACalcForCandle = new PgExecuter(connectionString, log).CheckMACalculationForOpenCandle(candle, calcType, dur);
                if (isExistMACalcForCandle > 0)
                {
                    log.Info("Предыдущий расчет существует. Он будет обновлен на новое значение");
                    new PgExecuter(connectionString, log).UpdateCalculationsTable(candle, calcType, dur, ma.ToString());


                }
                else
                {
                    log.Info("Предыдущий расчет НЕ существует.Записываю результат по MA в таблицу calculations");
                    new PgExecuter(connectionString, log).InsertIntoCalculationsTable(candle, calcType, dur, ma.ToString());
                }
            }
        }
    }
}
