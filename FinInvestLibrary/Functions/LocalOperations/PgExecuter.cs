using FinInvestLibrary.Objects;
using FinInvestLibrary.Objects.Calculations;
using FinInvestLibrary.Objects.StrategyObjects;
using FinInvestLibrary.Objects.Trade;
using log4net;
using Npgsql;

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

        public ShareObject GetShare(string figi)
        {
            var query_result = ExecuteReader("SELECT s.figi, s.ticker, s.class_code, s.isin, s.lot, s.currency, s.short_enabled_flag, s.name, s.exchange, s.issue_size, s.country_of_risk, s.country_of_risk_name, s.sector, s.issue_size_plan, s.trading_status, s.otc_flag, s.buy_available_flag, s.sell_available_flag, s.div_yield_flag, s.share_type, s.min_price_increment, s.api_trade_available_flag, s.uid, s.real_exchange, s.position_uid, s.for_iis_flag, s.for_qual_investor_flag, s.weekend_flag, s.blocked_tca_flag FROM public.Shares s where s.figi = '" + figi + "' limit 1");

            var partsOfRow = query_result[0].Split(';');
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

            return shObj;
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
                                @"' and EXTRACT(DOW from uhcas.candle_start_dt_utc) not in (0,6) and c.id is null and uhcas.is_closed_candle = true and uhcas.candle_start_dt_utc > '01.01.2018'";

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

        public List<FinInvestLibrary.Objects.Candle> GetCandlesWithoutCalculation(string calcType, string scale, string dur)
        {
            List<FinInvestLibrary.Objects.Candle> candleList = new List<FinInvestLibrary.Objects.Candle>();

            string sqlComm = @" select  uhcas.id, uhcas.scale, uhcas.figi, uhcas.candle_start_dt_utc, uhcas.open_price, uhcas.close_price, uhcas.max_price, uhcas.min_price, uhcas.volume
                                from public.union_history_candles_all_scales uhcas
                                left join public.calculations c on uhcas.id = c.candle_id" +
                                @" and c.calc_type = '" + calcType +
                                @"' and c.duration = '" + dur +
                                @"' where " +
                                @" uhcas.scale = '" + scale +
                                @"' and EXTRACT(DOW from uhcas.candle_start_dt_utc) not in (0,6) and c.id is null and uhcas.is_closed_candle = true and uhcas.candle_start_dt_utc > '01.01.2018'";

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
                                where uhcas.is_closed_candle = false and EXTRACT(DOW from uhcas.candle_start_dt_utc) not in (0,6) and scale = '" + scale + "' and figi = '" + share.figi + "'";

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

        public List<FinInvestLibrary.Objects.Candle> GetOpenCandles(string scale)
        {
            List<FinInvestLibrary.Objects.Candle> candleList = new List<FinInvestLibrary.Objects.Candle>();

            string sqlComm = @"select  uhcas.id, uhcas.scale, uhcas.figi, uhcas.candle_start_dt_utc, uhcas.open_price, uhcas.close_price, uhcas.max_price, uhcas.min_price, uhcas.volume
                                from public.union_history_candles_all_scales uhcas
                                where uhcas.is_closed_candle = false and EXTRACT(DOW from uhcas.candle_start_dt_utc) not in (0,6) and scale = '" + scale + "'";

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

            string sqlComm = "select * from union_history_candles_all_scales uhcas where uhcas.figi = '" + inputCandle.figi + "' and uhcas.scale = '1_day_scale' and EXTRACT(DOW from uhcas.candle_start_dt_utc) not in (0,6) and uhcas.candle_start_dt_utc <= '" + inputCandle.candle_start_dt + "' and EXTRACT(DOW from uhcas.candle_start_dt_utc) not in (0,6) order by uhcas.candle_start_dt_utc desc limit " + duration;

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
            string sqlComm = "insert into calculations(candle_id, figi, candle_scale, calc_type, duration, value, insertDate, updateDate) values(" + inputCandle.id + ", '" + inputCandle.figi + "', '" + inputCandle.scale + "', '" + calc_type + "', " + duration + ", '" + value + "', '" + dateTime.ToString() + "', '" + dateTime.ToString() + "')";
            return ExecuteNonQuery(sqlComm);

        }
        /// <summary>
        /// Запись объекта Calculation
        /// </summary>
        /// <param name="inputCalculation">Входной объект Calculation</param>
        /// <returns></returns>
        public bool InsertIntoCalculationsTable(FinInvestLibrary.Objects.Calculations.CalculationObject inputCalculation)
        {
            string value = inputCalculation.value.ToString().Replace(",", ".");
            string sqlComm = "insert into calculations(candle_id, figi, candle_scale, calc_type, duration, value, insertDate, updateDate) values(" + inputCalculation.candle_id + ", '" + inputCalculation.figi + "', '" + inputCalculation.candle_scale + "', '" + inputCalculation.calc_type + "', " + inputCalculation.duration + ", '" + value + "', '" + inputCalculation.insertdate + "', '" + inputCalculation.updatedate + "')";
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

        /// <summary>
        /// Обновление объекта Calculations 
        /// </summary>
        /// <param name="inputCalculation"></param>
        /// <returns></returns>
        public bool UpdateCalculationsTable(FinInvestLibrary.Objects.Calculations.CalculationObject inputCalculation)
        {
            DateTime dateTime = DateTime.Now;
            string value = inputCalculation.value.ToString().Replace(",", ".");
            string sqlComm = "update calculations set value = '" + value + "', updateDate = '" + dateTime.ToString() + "' where candle_id = " + inputCalculation.candle_id + " and calc_type = '" + inputCalculation.calc_type + "' and duration = " + inputCalculation.duration;
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
            //Результат ни когда не включает выходные дни.
            string sqlComm = "select value from calculations c left join union_history_candles_all_scales uhcas on uhcas.id = c.candle_id where c.calc_type = '" + calcType + "' and c.duration = " + duration + " and c.figi = '" + candle.figi + "' and uhcas.candle_start_dt_utc < '" + candle.candle_start_dt.Value + "' and EXTRACT(DOW from uhcas.candle_start_dt_utc) not in (0,6) order by uhcas.candle_start_dt_utc desc limit 1";
            string sqlResult = ExecuteScalarQuery(sqlComm);
            if (sqlResult != "SqlAnswerEmpty")
            {
                returnValue = float.Parse(sqlResult);
            }
            else
            {
                returnValue = -99999;
            }
            return returnValue;
        }

        public CalculationObject GeCurrentCalculation(FinInvestLibrary.Objects.Candle candle, string calcType, string duration)
        {
            string sqlComm = "select c.id,c.candle_id, c.figi, c.candle_scale, c.calc_type, c.duration, c.value, c.insertdate,c.updatedate from calculations c left join union_history_candles_all_scales uhcas on uhcas.id = c.candle_id where c.calc_type = '" + calcType + "' and c.duration = " + duration + " and c.figi = '" + candle.figi + "' and uhcas.candle_start_dt_utc = '" + candle.candle_start_dt.Value + "' and EXTRACT(DOW from uhcas.candle_start_dt_utc) not in (0,6) order by uhcas.candle_start_dt_utc desc limit 1";
            List<string> returnSqlResult = ExecuteReader(sqlComm);
            CalculationObject calculationObject = new CalculationObject();
            if (returnSqlResult.Count == 1)
            {
                var calculationStrSplit = returnSqlResult[0].Split(';');
                calculationObject.id = Convert.ToInt32(calculationStrSplit[0]);
                calculationObject.candle_id = Convert.ToInt32(calculationStrSplit[1]);
                calculationObject.figi = calculationStrSplit[2];
                calculationObject.candle_scale = calculationStrSplit[3];
                calculationObject.calc_type = calculationStrSplit[4];
                calculationObject.duration = Convert.ToInt32(calculationStrSplit[5]);
                calculationObject.value = float.Parse(calculationStrSplit[6]);
                calculationObject.insertdate = Convert.ToDateTime(calculationStrSplit[7]);
                calculationObject.updatedate = Convert.ToDateTime(calculationStrSplit[8]);
            }
            else
            {
                calculationObject = null;
            }
            return calculationObject;
        }

        /// <summary>
        /// Возвращает весь массив из репозитория calculations
        /// </summary>
        /// <returns>Список List с объектами calculations</returns>
        public List<CalculationObject> GetCalculations()
        {
            List<CalculationObject> calculations = new List<CalculationObject>();

            string sqlComm = "select id, candle_id, figi, candle_scale, calc_type, duration, value, insertdate, updatedate from calculations";
            List<string> returnSqlResult = ExecuteReader(sqlComm);
            foreach (string calculationStr in returnSqlResult)
            {
                CalculationObject calculationObject = new CalculationObject();
                var calculationStrSplit = calculationStr.Split(',');
                calculationObject.id = Convert.ToInt32(calculationStrSplit[0]);
                calculationObject.candle_id = Convert.ToInt32(calculationStrSplit[1]);
                calculationObject.figi = calculationStrSplit[2];
                calculationObject.candle_scale = calculationStrSplit[3];
                calculationObject.calc_type = calculationStrSplit[4];
                calculationObject.duration = Convert.ToInt32(calculationStrSplit[5]);
                calculationObject.value = float.Parse(calculationStrSplit[6]);
                calculationObject.insertdate = Convert.ToDateTime(calculationStrSplit[7]);
                calculationObject.updatedate = Convert.ToDateTime(calculationStrSplit[8]);
                calculations.Add(calculationObject);
            }
            return calculations;
        }

        /// <summary>
        /// Возвращает массив из репозитория calculations c фильтром по calcType
        /// </summary>
        /// <param name="calcType">Тип рассчета (MA/EMA/MACD/SL и прочее)</param>
        /// <returns></returns>
        public List<CalculationObject> GetCalculations(string calcType)
        {
            List<CalculationObject> calculations = new List<CalculationObject>();

            string sqlComm = "select id, candle_id, figi, candle_scale, calc_type, duration, value, insertdate, updatedate from calculations where calc_type = '" + calcType + "'";
            List<string> returnSqlResult = ExecuteReader(sqlComm);
            foreach (string calculationStr in returnSqlResult)
            {
                CalculationObject calculationObject = new CalculationObject();
                var calculationStrSplit = calculationStr.Split(',');
                calculationObject.id = Convert.ToInt32(calculationStrSplit[0]);
                calculationObject.candle_id = Convert.ToInt32(calculationStrSplit[1]);
                calculationObject.figi = calculationStrSplit[2];
                calculationObject.candle_scale = calculationStrSplit[3];
                calculationObject.calc_type = calculationStrSplit[4];
                calculationObject.duration = Convert.ToInt32(calculationStrSplit[5]);
                calculationObject.value = float.Parse(calculationStrSplit[6]);
                calculationObject.insertdate = Convert.ToDateTime(calculationStrSplit[7]);
                calculationObject.updatedate = Convert.ToDateTime(calculationStrSplit[8]);
                calculations.Add(calculationObject);
            }
            return calculations;
        }

        /// <summary>
        /// Возвращает массив из репозитория calculations c фильтром по calcType и duration
        /// </summary>
        /// <param name="calcType">Тип рассчета (MA/EMA/MACD/SL и прочее)</param>
        /// <param name="duration">Длительность (7, 12, 26 и другие)</param>
        /// <returns></returns>
        public List<CalculationObject> GetCalculations(string calcType, string duration)
        {
            List<CalculationObject> calculations = new List<CalculationObject>();

            string sqlComm = "select id, candle_id, figi, candle_scale, calc_type, duration, value, insertdate, updatedate from calculations where calc_type = '" + calcType + "' and duration = " + duration;
            List<string> returnSqlResult = ExecuteReader(sqlComm);
            try
            {
                foreach (string calculationStr in returnSqlResult)
                {
                    CalculationObject calculationObject = new CalculationObject();
                    var calculationStrSplit = calculationStr.Split(';');
                    calculationObject.id = Convert.ToInt32(calculationStrSplit[0]);
                    calculationObject.candle_id = Convert.ToInt32(calculationStrSplit[1]);
                    calculationObject.figi = calculationStrSplit[2];
                    calculationObject.candle_scale = calculationStrSplit[3];
                    calculationObject.calc_type = calculationStrSplit[4];
                    calculationObject.duration = Convert.ToInt32(calculationStrSplit[5]);
                    calculationObject.value = float.Parse(calculationStrSplit[6]);
                    calculationObject.insertdate = Convert.ToDateTime(calculationStrSplit[7]);
                    calculationObject.updatedate = Convert.ToDateTime(calculationStrSplit[8]);
                    calculations.Add(calculationObject);
                }
            }
            catch (Exception ex)
            {
                log.Error(ex.ToString());
            }

            return calculations;
        }

        /// <summary>
        /// Возвращает массив из репозитория calculations c фильтром по calcType и duration с фильтром по типу свечи (закрытая или открытая) и массштабу свечи
        /// </summary>
        /// <param name="calcType"></param>
        /// <param name="duration"></param>
        /// <returns></returns>
        public List<CalculationObject> GetCalculations(string calcType, string duration, string candleScale, bool isClosedCanldes)
        {
            List<CalculationObject> calculations = new List<CalculationObject>();
            string sqlComm = "with candles as (select * from union_history_candles_all_scales uhcas where uhcas.is_closed_candle = " + isClosedCanldes.ToString() + " and uhcas.scale = '" + candleScale + "' and EXTRACT(DOW from uhcas.candle_start_dt_utc) not in (0,6)) select c.* from candles cndls join calculations c on cndls.id = c.candle_id and c.calc_type = '" + calcType + "' and c.duration = " + duration;
            List<string> returnSqlResult = ExecuteReader(sqlComm);
            try
            {
                foreach (string calculationStr in returnSqlResult)
                {
                    CalculationObject calculationObject = new CalculationObject();
                    var calculationStrSplit = calculationStr.Split(';');
                    calculationObject.id = Convert.ToInt32(calculationStrSplit[0]);
                    calculationObject.candle_id = Convert.ToInt32(calculationStrSplit[1]);
                    calculationObject.figi = calculationStrSplit[2];
                    calculationObject.candle_scale = calculationStrSplit[3];
                    calculationObject.calc_type = calculationStrSplit[4];
                    calculationObject.duration = Convert.ToInt32(calculationStrSplit[5]);
                    calculationObject.value = float.Parse(calculationStrSplit[6]);
                    calculationObject.insertdate = Convert.ToDateTime(calculationStrSplit[7]);
                    calculationObject.updatedate = Convert.ToDateTime(calculationStrSplit[8]);
                    calculations.Add(calculationObject);
                }
            }
            catch (Exception ex)
            {
                log.Error(ex.ToString());
            }

            return calculations;
        }

        public CalculationObject GetCalculations(string candle_id, string calc_type, string duration, string scale)
        {
            var calculationObject = new CalculationObject();

            string sqlComm = "select c.id, c.candle_id, c.figi, c.candle_scale, c.calc_type, c.duration, c.value, c.insertdate, c.updatedate  from calculations c where c.candle_id = " + candle_id + " and  c.calc_type = '" + calc_type + "' and c.duration = " + duration + " and c.candle_scale = '" + scale + "'";
            List<string> returnSqlResult = ExecuteReader(sqlComm);

            if (returnSqlResult.Count == 1)
            {
                var calculationStrSplit = returnSqlResult[0].Split(';');
                calculationObject.id = Convert.ToInt32(calculationStrSplit[0]);
                calculationObject.candle_id = Convert.ToInt32(calculationStrSplit[1]);
                calculationObject.figi = calculationStrSplit[2];
                calculationObject.candle_scale = calculationStrSplit[3];
                calculationObject.calc_type = calculationStrSplit[4];
                calculationObject.duration = Convert.ToInt32(calculationStrSplit[5]);
                calculationObject.value = float.Parse(calculationStrSplit[6]);
                calculationObject.insertdate = Convert.ToDateTime(calculationStrSplit[7]);
                calculationObject.updatedate = Convert.ToDateTime(calculationStrSplit[8]);
            }
            else if (returnSqlResult.Count > 1)
            {
                log.Error("Запрос вернул больше 1-го вычисления. Проверьте данные в репозитории вычислений");
                log.Error(sqlComm);
                log.Error("Полученные строки:");
                foreach (var item in returnSqlResult)
                {
                    log.Error(item.ToString());
                }
            }
            else
            {
                calculationObject = null;
            }
            return calculationObject;
        }

        /// <summary>
        /// Возвращает набор вычислений за текущий день
        /// </summary>
        /// <param name="calcType"></param>
        /// <param name="duration"></param>
        /// <returns></returns>
        public List<CalculationObject> GetCurrentCalculations(string calcType, string duration)
        {
            List<CalculationObject> calculations = new List<CalculationObject>();
            string sqlComm = "select * from calculations c where c.calc_type = '" + calcType + "' and c.duration = " + duration + " and c.candle_scale = '1_day_scale' and insertdate > current_date";
            List<string> returnSqlResult = ExecuteReader(sqlComm);
            try
            {
                foreach (var item in returnSqlResult)
                {
                    var calculationObject = new CalculationObject();
                    var calculationStrSplit = item.Split(';');
                    calculationObject.id = Convert.ToInt32(calculationStrSplit[0]);
                    calculationObject.candle_id = Convert.ToInt32(calculationStrSplit[1]);
                    calculationObject.figi = calculationStrSplit[2];
                    calculationObject.candle_scale = calculationStrSplit[3];
                    calculationObject.calc_type = calculationStrSplit[4];
                    calculationObject.duration = Convert.ToInt32(calculationStrSplit[5]);
                    calculationObject.value = float.Parse(calculationStrSplit[6]);
                    calculationObject.insertdate = Convert.ToDateTime(calculationStrSplit[7]);
                    calculationObject.updatedate = Convert.ToDateTime(calculationStrSplit[8]);
                    calculations.Add(calculationObject);
                }
            }
            catch (Exception ex)
            {
                log.Error("В процессе чтения вычислений произошла ошибка");
                log.Error(ex);
                calculations = new List<CalculationObject>();
            }
            return calculations;
        }

        /// <summary>
        /// Возвращает набор вычислений за предыдущий день
        /// </summary>
        /// <param name="calcType"></param>
        /// <param name="duration"></param>
        /// <returns></returns>
        public List<CalculationObject> GetPreviousCalculations(string calcType, string duration)
        {
            List<CalculationObject> calculations = new List<CalculationObject>();
            string sqlComm = "select * from calculations c where c.calc_type = '" + calcType + "' and c.duration = " + duration + " and c.candle_scale = '1_day_scale' and insertdate > current_date";
            List<string> returnSqlResult = ExecuteReader(sqlComm);
            try
            {
                foreach (var item in returnSqlResult)
                {
                    var calculationObject = new CalculationObject();
                    var calculationStrSplit = item.Split(';');
                    calculationObject.id = Convert.ToInt32(calculationStrSplit[0]);
                    calculationObject.candle_id = Convert.ToInt32(calculationStrSplit[1]);
                    calculationObject.figi = calculationStrSplit[2];
                    calculationObject.candle_scale = calculationStrSplit[3];
                    calculationObject.calc_type = calculationStrSplit[4];
                    calculationObject.duration = Convert.ToInt32(calculationStrSplit[5]);
                    calculationObject.value = float.Parse(calculationStrSplit[6]);
                    calculationObject.insertdate = Convert.ToDateTime(calculationStrSplit[7]);
                    calculationObject.updatedate = Convert.ToDateTime(calculationStrSplit[8]);
                    calculations.Add(calculationObject);
                }
            }
            catch (Exception ex)
            {
                log.Error("В процессе чтения вычислений произошла ошибка");
                log.Error(ex);
                calculations = new List<CalculationObject>();
            }
            return calculations;
        }

        /// <summary>
        /// Возвращает вычисление для определенной свечи
        /// </summary>
        /// <param name="candle"></param>
        /// <param name="calcType"></param>
        /// <param name="duration"></param>
        /// <returns></returns>
        public CalculationObject GetPreviousCalculation(FinInvestLibrary.Objects.Candle candle, string calcType, string duration)
        {
            string sqlComm = "select c.id,c.candle_id, c.figi, c.candle_scale, c.calc_type, c.duration, c.value, c.insertdate,c.updatedate from calculations c left join union_history_candles_all_scales uhcas on uhcas.id = c.candle_id where c.calc_type = '" + calcType + "' and c.duration = " + duration + " and c.figi = '" + candle.figi + "' and uhcas.candle_start_dt_utc < '" + candle.candle_start_dt.Value + "' and EXTRACT(DOW from uhcas.candle_start_dt_utc) not in (0,6) order by uhcas.candle_start_dt_utc desc limit 1";
            List<string> returnSqlResult = ExecuteReader(sqlComm);
            CalculationObject calculationObject = new CalculationObject();
            if (returnSqlResult.Count == 1)
            {
                var calculationStrSplit = returnSqlResult[0].Split(';');
                calculationObject.id = Convert.ToInt32(calculationStrSplit[0]);
                calculationObject.candle_id = Convert.ToInt32(calculationStrSplit[1]);
                calculationObject.figi = calculationStrSplit[2];
                calculationObject.candle_scale = calculationStrSplit[3];
                calculationObject.calc_type = calculationStrSplit[4];
                calculationObject.duration = Convert.ToInt32(calculationStrSplit[5]);
                calculationObject.value = float.Parse(calculationStrSplit[6]);
                calculationObject.insertdate = Convert.ToDateTime(calculationStrSplit[7]);
                calculationObject.updatedate = Convert.ToDateTime(calculationStrSplit[8]);
            }
            else
            {
                calculationObject = null;
            }
            return calculationObject;
        }
        /// <summary>
        /// Проверяет наличие актива в листе исключений excluded_shares
        /// </summary>
        /// <param name="figi"></param>
        /// <returns></returns>
        public bool CheckShareByExclude(string figi)
        {
            bool returnValue = false;
            string sqlComm = "select count(*) from excluded_shares es where es.figi = '" + figi + "'";
            string sqlResult = ExecuteScalarQuery(sqlComm);
            try
            {
                int sqlResultInt = Convert.ToInt32(sqlResult);
                if (sqlResultInt > 0) { returnValue = true; }
            }
            catch (Exception ex)
            {
                log.Error("Возникла ошибка в процессе запроса листа исключений");
                log.Error(ex);
                returnValue = false;
            }
            return returnValue;
        }

        /// <summary>
        /// Проверка на наличие активной ТИ по входным параметрам
        /// </summary>
        /// <param name="figi"></param>
        /// <param name="strat_name"></param>
        /// <param name="strat_type"></param>
        /// <returns></returns>
        public bool CheckTrades(string figi, string strat_name, string strat_type)
        {
            bool returnValue = false;
            string sqlComm = "select count(*) from trades t where figi = '" + figi + "' and stratname = '" + strat_name + "' and tradetype = '" + strat_type + "'";
            string sqlResult = ExecuteScalarQuery(sqlComm);
            try
            {
                int sqlResultInt = Convert.ToInt32(sqlResult);
                if (sqlResultInt > 0) { returnValue = true; }
            }
            catch (Exception ex)
            {
                log.Error("Возникла ошибка в процессе запроса активных идей");
                log.Error(ex);
                returnValue = false;
            }
            return returnValue;
        }

        /// <summary>
        /// Получаю граничные условия для торговых идей
        /// </summary>
        /// <param name="figi"></param>
        /// <param name="strat_name"></param>
        /// <param name="strat_duration"></param>
        /// <returns></returns>
        public BorderPoints GetPersonalBorderPoints(string figi, string strat_name, string strat_duration)
        {
            string sqlComm = "select figi, strat_name, strat_type, strat_duration, stop_loss_point, take_profit_point from cfg_border_points_for_strategys where figi = '" + figi + "' and strat_name = '" + strat_name + "' and strat_duration =  '" + strat_duration + "'";
            List<string> sqlResult = ExecuteReader(sqlComm);
            BorderPoints borderPoints = new BorderPoints();
            if (sqlResult.Count > 0)
            {
                string stop_loss_point_str = sqlResult[0].ToString().Split(";")[4];
                string take_profit_point_str = sqlResult[0].ToString().Split(";")[5];



                borderPoints.figi = figi;
                borderPoints.strat_name = strat_name;
                borderPoints.strat_duration = strat_duration;

                if (stop_loss_point_str.Length > 5)
                {
                    stop_loss_point_str = stop_loss_point_str.Substring(0, 5);
                    borderPoints.stop_loss_point = float.Parse(stop_loss_point_str);
                }
                else
                {
                    borderPoints.stop_loss_point = float.Parse(stop_loss_point_str);
                }

                if (take_profit_point_str.Length > 5)
                {
                    take_profit_point_str = take_profit_point_str.Substring(0, 5);
                    borderPoints.take_profit_point = float.Parse(take_profit_point_str);
                }
                else
                {
                    borderPoints.take_profit_point = float.Parse(take_profit_point_str);
                }

            }
            else
            {
                borderPoints.figi = figi;
                borderPoints.strat_name = strat_name;
                borderPoints.strat_duration = strat_duration;
                borderPoints.stop_loss_point = float.Parse("-0,03");
                borderPoints.take_profit_point = float.Parse("0,05");

            }
            return borderPoints;
        }

        public string GetTicker(string figi)
        {
            string sqlComm = "select ticker from shares where figi = '" + figi + "'";
            string ticker = ExecuteScalarQuery(sqlComm);
            return ticker;
        }

        public bool AddNewTradeIdea(TradeObject tradeObject)
        {
            string sqlComm = "INSERT INTO public.trades (tradeId,tradeType,stratName,openCandleId,openCandleDt,figi,tradeStartDt,openTradePrice,maxTradePrice,minTradePrice,maxtradepricecandleid,maxtradepricecandledt,mintradepricecandleid,mintradepricecandledt,calculatetype,target1Value,target2Value,stopLoss1Value,stopLoss2Value, trade_is_close_analytic, trade_is_close_communication, target1CloseCause, target2CloseCause) VALUES('"
                + tradeObject.tradeId + "','"
                + tradeObject.tradeType + "','"
                + tradeObject.stratName + "','"
                + tradeObject.openCandleId + "','"
                + tradeObject.openCandleDt + "','"
                + tradeObject.figi + "','"
                + tradeObject.tradeStartDt + "','"
                + tradeObject.openTradePrice.ToString().Replace(',', '.') + "','"
                + tradeObject.maxTradePrice.ToString().Replace(',', '.') + "','"
                + tradeObject.minTradePrice.ToString().Replace(',', '.') + "',"
                + tradeObject.maxtradepricecandleid + ",'"
                + tradeObject.maxtradepricecandledt + "',"
                + tradeObject.mintradepricecandleid + ",'"
                + tradeObject.mintradepricecandledt + "','"
                + tradeObject.tradeType + "','"
                + tradeObject.target1Value.ToString().Replace(',', '.') + "','"
                + tradeObject.target2Value.ToString().Replace(',', '.') + "','"
                + tradeObject.stopLoss1Value.ToString().Replace(',', '.') + "','"
                + tradeObject.stopLoss2Value.ToString().Replace(',', '.') + "',"
                + tradeObject.trade_is_close_analytic + ","
                + tradeObject.trade_is_close_communication + ",'"
                + tradeObject.target1CloseCause + "','"
                + tradeObject.target2CloseCause + "')";

            return ExecuteNonQuery(sqlComm);
        }

        public bool AddNewCommunication(string json, TradeObject tradeObject)
        {
            string sqlComm = "INSERT INTO public.communications (id,external_id,create_dt,message_content) VALUES ('" + Guid.NewGuid().ToString().Replace("-", "") + "','" + tradeObject.tradeId + "','" + DateTime.Now.ToString() + "','" + json + "')";
            return ExecuteNonQuery(sqlComm);
        }

        /// <summary>
        /// Возвращает найденные сделки в соответсвии с входными параметрами
        /// </summary>
        /// <param name="stratName"></param>
        /// <param name="tradeType"></param>
        /// <returns></returns>
        public List<TradeObject> GetActiveTrades(string stratName, string tradeType)
        {
            string sqlCommand = "select tradeId,tradeType,stratName,openCandleId,openCandleDt,figi,tradeStartDt,openTradePrice, maxTradePrice, minTradePrice,maxtradepricecandleid,maxtradepricecandledt,mintradepricecandleid,mintradepricecandledt,calculatetype, target1Value, target2Value, stopLoss1Value, stopLoss2Value, target1ClosePrice, target2ClosePrice, target1CloseDT, target2CloseDT, target1CloseCause, target2CloseCause, trade_is_close_analytic, trade_is_close_communication  from trades where trade_is_close_communication is false and stratname = '" + stratName + "' and tradetype = '" + tradeType + "'";
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
            return tradeObjectList;
        }

        //Обновление торговой идеи
        public bool UpdateTarget1Trade(TradeObject trade)
        {
            string sqlComm = "UPDATE public.trades t SET target1ClosePrice = " + trade.target1ClosePrice.ToString().Replace(',', '.')
                                + ", target1CloseDT = '" + trade.target1CloseDT.ToString()
                                + "', target1CloseCause = '" + trade.target1CloseCause
                                + "', stopLoss2Value = " + trade.stopLoss2Value.ToString().Replace(',', '.') + " WHERE t.tradeid = '" + trade.tradeId + "'";
            return ExecuteNonQuery(sqlComm);
        }

        public bool UpdateTarget2Trade(TradeObject trade)
        {
            string sqlComm = "UPDATE public.trades t SET target2ClosePrice = " + trade.target2ClosePrice.ToString().Replace(',', '.')
                                + ", target2CloseDT = '" + trade.target2CloseDT.ToString()
                                + "', target2CloseCause = '" + trade.target2CloseCause
                                + "', trade_is_close_communication = '" + trade.trade_is_close_communication
                                + "', closecandleid = " + trade.candleObject.id
                                + ", closecandledt = '" + trade.candleObject.candle_start_dt.ToString()
                                + "', tradeclosedt = '" + trade.tradeCloseDt.ToString()
                                + "', closetradeprice = '" + trade.candleObject.close_price.ToString().Replace(',', '.')
                                + "' WHERE t.tradeid = '" + trade.tradeId + "'";
            return ExecuteNonQuery(sqlComm);
        }

        public bool UpdateStopLoss1Trade(TradeObject trade)
        {
            string sqlComm = "UPDATE public.trades t SET target1ClosePrice = " + trade.target1ClosePrice.ToString().Replace(',', '.')
                        + ", target1CloseDT = '" + trade.target1CloseDT.ToString()
                        + "', target1CloseCause = '" + trade.target1CloseCause
                        + "', trade_is_close_communication = '" + trade.trade_is_close_communication
                        + "', stopLoss2Value = " + trade.stopLoss2Value.ToString().Replace(',', '.') + " WHERE t.tradeid = '" + trade.tradeId + "'";
            return ExecuteNonQuery(sqlComm);
        }

        public bool UpdateStopLoss2Trade(TradeObject trade)
        {
            string sqlComm = "UPDATE public.trades t SET target2ClosePrice = " + trade.target2ClosePrice.ToString().Replace(',', '.')
                                + ", target2CloseDT = '" + trade.target2CloseDT.ToString()
                                + "', target2CloseCause = '" + trade.target2CloseCause
                                + "', trade_is_close_communication = '" + trade.trade_is_close_communication
                                + "', closecandleid = " + trade.candleObject.id
                                + ", closecandledt = '" + trade.candleObject.candle_start_dt.ToString()
                                + "', tradeclosedt = '" + trade.tradeCloseDt.ToString()
                                + "', closetradeprice = '" + trade.candleObject.close_price.ToString().Replace(',', '.')
                                + "' WHERE t.tradeid = '" + trade.tradeId + "'";
            return ExecuteNonQuery(sqlComm);
        }



    }

}
