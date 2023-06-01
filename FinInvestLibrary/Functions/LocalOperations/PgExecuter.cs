using FinInvestLibrary.Objects;
using Google.Protobuf.WellKnownTypes;
using log4net;
using Microsoft.VisualBasic;
using Npgsql;
using System.Collections.Generic;
using Tinkoff.InvestApi.V1;
using static System.Formats.Asn1.AsnWriter;

namespace FinInvestLibrary.Functions.LocalOperations
{
    public class PgExecuter
    {
        private string connectionString = string.Empty;
        private NpgsqlConnection connection = null;
        private ILog log = null;
        /// <summary>
        /// Инцициализация класса
        /// </summary>
        /// <param name="connString">строка подключения к бд</param>
        /// <param name="log">объект логгера для низкоуровнего логирования</param>
        public PgExecuter(string connString, ILog inputlog)
        {
            connectionString = connString;
            connection = new NpgsqlConnection(connString);
            log = inputlog;
        }

        public bool ExecuteNonQuery(string SQLCommand)
        {
            bool result = false;
            log.Debug("Подключаюсь к БД...");
            try
            {

                connection.Open();
                log.Debug("Подключение устрановлено");
            }
            catch (Exception ex)
            {
                log.Error("Не удалось установить подключение");
                log.Error(ex.ToString());
                return result;
            }
            DateTime executeStartDT = DateTime.Now.ToUniversalTime();
            try
            {
                log.Debug("Выполняю полученную инструкцию SQL");
                log.Debug(SQLCommand);
                using var command = new NpgsqlCommand(SQLCommand, connection);
                command.CommandTimeout = 600;
                command.ExecuteNonQuery();

                result = true;
            }
            catch (Exception ex)
            {
                log.Error("Не удалось выполнить инструкцию");
                log.Error(ex.ToString());
                connection.Close();
                return result;
            }
            DateTime executeFinishDT = DateTime.Now.ToUniversalTime();
            log.Debug("Инструкция успешно выполнена");
            log.Debug("Время выполнения: " + (executeFinishDT - executeStartDT).TotalSeconds + " секунд.");
            connection.Close();
            return result;
        }

        public string ExecuteScalarQuery(string SQLCommand)
        {
            string returnStr = string.Empty;

            log.Debug("Подключаюсь к БД...");
            try
            {

                connection.Open();
                log.Debug("Подключение устрановлено");
            }
            catch (Exception ex)
            {
                log.Error("Не удалось установить подключение");
                log.Error(ex.ToString());
                connection.Close();
                return returnStr;
            }

            DateTime executeStartDT = DateTime.Now.ToUniversalTime();

            try
            {
                log.Debug("Выполняю полученную инструкцию SQL");
                log.Debug(SQLCommand);
                using var command = new NpgsqlCommand(SQLCommand, connection);
                command.CommandTimeout = 600;
                var res = command.ExecuteScalar();
                
                if (res == null)
                {
                    returnStr = "SqlAnswerEmpty";
                }
                else
                {
                    returnStr = res.ToString();
                }
                connection.Close();

            }
            catch (Exception ex)
            {
                log.Error("Не удалось выполнить инструкцию");
                log.Error(ex.ToString());
                connection.Close();
            }
            DateTime executeFinishDT = DateTime.Now.ToUniversalTime();
            log.Debug("Инструкция успешно выполнена");
            log.Debug("Время выполнения: " + (executeFinishDT - executeStartDT).TotalSeconds + " секунд.");
            return returnStr;

        }

        public List<string> ExecuteReader(string SQLCommand)
        {
            List<string> returnListString = new List<string>();
            log.Debug("Подключаюсь к БД...");
            try
            {
                connection.Open();
                log.Debug("Подключение устрановлено");
            }
            catch (Exception ex)
            {
                log.Error("Не удалось установить подключение");
                log.Error(ex.ToString());
                connection.Close();
            }

            DateTime executeStartDT = DateTime.Now.ToUniversalTime();
            try
            {
                log.Debug("Выполняю полученную инструкцию SQL");
                log.Debug(SQLCommand);
                using var command = new NpgsqlCommand(SQLCommand, connection);
                command.CommandTimeout = 600;
                using NpgsqlDataReader reader = command.ExecuteReader();

                while (reader.Read())
                {
                    string row = string.Empty;
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row = row + reader[i].ToString();
                        if (i != reader.FieldCount)
                        {
                            row = row + ";";
                        }
                    }
                    returnListString.Add(row);
                }
                connection.Close();

            }
            catch (Exception ex)
            {
                log.Error("Не удалось выполнить инструкцию");
                log.Error(ex.ToString());
                connection.Close();
                return returnListString;
            }
            DateTime executeFinishDT = DateTime.Now.ToUniversalTime();
            log.Debug("Инструкция успешно выполнена");
            log.Debug("Время выполнения: " + (executeFinishDT - executeStartDT).TotalSeconds + " секунд.");

            return returnListString;


        }

        /// <summary>
        /// Возвращает список инструментов из таблицы shares (акции)
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        public List<ShareObject> GetActualSharesList()
        {
            var query_result = ExecuteReader("SELECT s.figi, s.ticker, s.class_code, s.isin, s.lot, s.currency, s.short_enabled_flag, s.name, s.exchange, s.issue_size, s.country_of_risk, s.country_of_risk_name, s.sector, s.issue_size_plan, s.trading_status, s.otc_flag, s.buy_available_flag, s.sell_available_flag, s.div_yield_flag, s.share_type, s.min_price_increment, s.api_trade_available_flag, s.uid, s.real_exchange, s.position_uid, s.for_iis_flag, s.for_qual_investor_flag, s.weekend_flag, s.blocked_tca_flag, es.ticker as excluded_ticker FROM public.Shares s left join excluded_shares es on (es.ticker = s.ticker or es.isin = s.isin or es.figi = s.figi)");
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
        public List<FinInvestLibrary.Objects.Candle> GetCandlesWithoutCalculation(string calcType, string scale, ShareObject share, string dur)
        {
            List<FinInvestLibrary.Objects.Candle> candleList = new List<FinInvestLibrary.Objects.Candle>();   

            string sqlComm = @" select  uhcas.id, uhcas.scale, uhcas.figi, uhcas.candle_start_dt_utc, uhcas.open_price, uhcas.close_price, uhcas.max_price, uhcas.min_price, uhcas.volume
                                from public.union_history_candles_all_scales uhcas
                                left join public.calculations c on uhcas.id = c.candle_id" +
                                @" and c.calc_type = '" + calcType +
                                @"' and c.duration = '" + dur +
                                @"' where uhcas.figi = '" + share.figi +
                                @"' and uhcas.scale = '" + scale +
                                @"' and c.id is null and uhcas.is_closed_candle = true and uhcas.candle_start_dt_utc > '01.01.2018'";

            var candlesStrings = ExecuteReader(sqlComm);

            foreach (var candleStr in candlesStrings)
            {
                FinInvestLibrary.Objects.Candle candle = new FinInvestLibrary.Objects.Candle();
                var candleStrResult = candleStr.Split(";").ToList();
                candle.id = Convert.ToInt32(candleStrResult[0]);
                candle.scale = candleStrResult[1];
                candle.figi = candleStrResult[2];
                candle.candle_start_dt = Convert.ToDateTime(candleStrResult[3]);
                candle.open_price = float.Parse(candleStrResult[4]);
                candle.close_price = float.Parse(candleStrResult[5]);
                candle.max_price = float.Parse(candleStrResult[6]);
                candle.min_price = float.Parse(candleStrResult[7]);
                candle.volume = Convert.ToInt32(candleStrResult[8]);

                candleList.Add(candle);
            }

            return candleList;
        }

        public List<FinInvestLibrary.Objects.Candle> GetOpenCandles(string scale, ShareObject share)
        {
            List<FinInvestLibrary.Objects.Candle> candleList = new List<FinInvestLibrary.Objects.Candle>();

            string sqlComm = @"select  uhcas.id, uhcas.scale, uhcas.figi, uhcas.candle_start_dt_utc, uhcas.open_price, uhcas.close_price, uhcas.max_price, uhcas.min_price, uhcas.volume
                                from public.union_history_candles_all_scales uhcas
                                where uhcas.is_closed_candle = false and scale = '" + scale + "' and figi = '"+ share.figi + "'";

            var candlesStrings = ExecuteReader(sqlComm);

            foreach (var candleStr in candlesStrings)
            {
                FinInvestLibrary.Objects.Candle candle = new FinInvestLibrary.Objects.Candle();
                var candleStrResult = candleStr.Split(";").ToList();
                candle.id = Convert.ToInt32(candleStrResult[0]);
                candle.scale = candleStrResult[1];
                candle.figi = candleStrResult[2];
                candle.candle_start_dt = Convert.ToDateTime(candleStrResult[3]);
                candle.open_price = float.Parse(candleStrResult[4]);
                candle.close_price = float.Parse(candleStrResult[5]);
                candle.max_price = float.Parse(candleStrResult[6]);
                candle.min_price = float.Parse(candleStrResult[7]);
                candle.volume = Convert.ToInt32(candleStrResult[8]);

                candleList.Add(candle);
            }

            return candleList;
        }

        public List<FinInvestLibrary.Objects.Candle> GetCandlesForCalc(FinInvestLibrary.Objects.Candle inputCandle, string duration)
        { 
            List<FinInvestLibrary.Objects.Candle> candleList = new List<FinInvestLibrary.Objects.Candle>();

            string sqlComm = "select * from union_history_candles_all_scales uhcas where uhcas.figi = '" + inputCandle.figi + "' and uhcas.scale = '1_day_scale' and uhcas.candle_start_dt_utc <= '" + inputCandle.candle_start_dt + "' and EXTRACT(DOW from uhcas.candle_start_dt_utc) not in (0,6) order by uhcas.candle_start_dt_utc desc limit " + duration ;

            var candlesStrings = ExecuteReader(sqlComm);

            foreach (var candleStr in candlesStrings)
            {
                FinInvestLibrary.Objects.Candle candle = new FinInvestLibrary.Objects.Candle();
                var candleStrResult = candleStr.Split(";").ToList();
                candle.id = Convert.ToInt32(candleStrResult[0]);
                candle.scale = candleStrResult[1];
                candle.figi = candleStrResult[2];
                candle.candle_start_dt = Convert.ToDateTime(candleStrResult[3]);
                candle.open_price = float.Parse(candleStrResult[4]);
                candle.close_price = float.Parse(candleStrResult[5]);
                candle.max_price = float.Parse(candleStrResult[6]);
                candle.min_price = float.Parse(candleStrResult[7]);
                candle.volume = Convert.ToInt32(candleStrResult[8]);

                candleList.Add(candle);
            }

            return candleList;

        }

        public bool InsertIntoCalculationsTable(FinInvestLibrary.Objects.Candle inputCandle, string calc_type, int duration, string calc_result)
        {
            DateTime dateTime = DateTime.Now;
            string value = calc_result.Replace(",", ".");
            string sqlComm = "insert into calculations(candle_id, figi, candle_scale, calc_type, duration, value, insertDate, updateDate) values(" + inputCandle.id + ", '" + inputCandle.figi + "', '" + inputCandle.scale + "', '" + calc_type + "', " + duration + ", '" + value + "', '" + dateTime.ToString() + "', '"+dateTime.ToString() + "')";
            return ExecuteNonQuery(sqlComm);
            
        }

        public bool UpdateCalculationsTable(FinInvestLibrary.Objects.Candle inputCandle, string calc_type, int duration, string calc_result)
        {
            DateTime dateTime = DateTime.Now;
            string value = calc_result.Replace(",", ".");
            string sqlComm = "update calculations set value = '" + value + "', updateDate = '" + dateTime.ToString() + "' where candle_id = " + inputCandle.id + " and calc_type = '" + calc_type + "' and duration = " + duration;
            //string sqlComm = "insert into calculations(candle_id, figi, candle_scale, calc_type, duration, value, insertDate, updateDate) values(" + inputCandle.id + ", '" + inputCandle.figi + "', '" + inputCandle.scale + "', '" + calc_type + "', " + duration + ", '" + value + "', '" + dateTime.ToString() + "', '" + dateTime.ToString() + "')";
            return ExecuteNonQuery(sqlComm);

        }

        public int CheckMACalculationForOpenCandle(FinInvestLibrary.Objects.Candle candle, string calc_type, int duration)
        {
            int returnValue = -1;
            string sqlComm = "select count(*) from calculations where candle_id = " + candle.id + " and calc_type = '" + calc_type + "' and duration = " + duration;
            string returnValueStr = ExecuteScalarQuery(sqlComm);
            returnValue = Convert.ToInt32(returnValueStr);
            return returnValue;
        }

        public float GetPreviousValue(FinInvestLibrary.Objects.Candle candle, string duration, string calcType)
        {
            float returnValue;
            
            string sqlComm = "select value from calculations c left join union_history_candles_all_scales uhcas on uhcas.id = c.candle_id where c.calc_type = '" + calcType + "' and c.duration = " + duration + " and c.figi = '" + candle.figi + "' and uhcas.candle_start_dt_utc = '" + candle.candle_start_dt.Value.AddDays(-1) + "'";
            string sqlResult = ExecuteScalarQuery(sqlComm);
            if (sqlResult != "SqlAnswerEmpty")
            {
                returnValue = float.Parse(sqlResult);
            }
            else
            {
                returnValue = -1;
            }
            return returnValue;
        }
    }
}
