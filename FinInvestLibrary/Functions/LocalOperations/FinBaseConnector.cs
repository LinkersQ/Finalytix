using FinInvestLibrary.Objects;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FinInvestLibrary.Functions.LocalOperations
{
    public class FinBaseConnector
    {
        /// <summary>
        /// Запрос существующих инструментов
        /// </summary>
        /// <param name="connString">строка подключения к базе данных</param>
        /// <returns></returns>
        public List<ShareObject> GetSharesFromDB(string connString)
        {
            List<ShareObject> shObjList = new List<ShareObject>();
            Boolean allOK = true;

            using var connection = new NpgsqlConnection(connString);
            try
            {
                connection.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                allOK = false;
            }

            if (allOK)
            {
                var dBRequest = "SELECT figi, ticker, class_code, isin, lot, currency, short_enabled_flag, name, exchange, issue_size, country_of_risk, country_of_risk_name, sector, issue_size_plan, trading_status, otc_flag, buy_available_flag, sell_available_flag, div_yield_flag, share_type, min_price_increment, api_trade_available_flag, uid, real_exchange, position_uid, for_iis_flag, for_qual_investor_flag, weekend_flag, blocked_tca_flag FROM public.Shares";
                try
                {
                    using var command = new NpgsqlCommand(dBRequest, connection);
                    using NpgsqlDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        ShareObject shObj = new ShareObject();
                        shObj.figi = reader.GetString(0);
                        shObj.ticker = reader.GetString(1);
                        shObj.class_code = reader.GetString(2);
                        shObj.isin = reader.GetString(3);
                        shObj.lot = reader.GetInt32(4);
                        shObj.currency = reader.GetString(5);
                        shObj.short_enabled_flag = reader.GetBoolean(6);
                        shObj.name = reader.GetString(7);
                        shObj.exchange = reader.GetString(8);
                        shObj.issue_size = reader.GetInt64(9);
                        shObj.country_of_risk = reader.GetString(10);
                        shObj.country_of_risk_name = reader.GetString(11);
                        shObj.sector = reader.GetString(12);
                        shObj.issue_size_plan = reader.GetInt64(13);
                        shObj.trading_status = reader.GetString(14);
                        shObj.otc_flag = reader.GetBoolean(15);
                        shObj.buy_available_flag = reader.GetBoolean(16);
                        shObj.sell_available_flag = reader.GetBoolean(17);
                        shObj.div_yield_flag = reader.GetBoolean(18);
                        shObj.share_type = reader.GetString(19);
                        shObj.min_price_increment = (float)reader.GetDouble(20);
                        shObj.api_trade_available_flag = reader.GetBoolean(21);
                        shObj.uid = reader.GetString(22);
                        shObj.real_exchange = reader.GetString(23);
                        shObj.position_uid = reader.GetString(24);
                        shObj.for_iis_flag = reader.GetBoolean(25);
                        shObj.for_qual_investor_flag = reader.GetBoolean(26);
                        shObj.weekend_flag = reader.GetBoolean(27);
                        shObj.blocked_tca_flag = reader.GetBoolean(28);

                        shObjList.Add(shObj);

                    }
                }
                catch (Exception ex)
                {
                   
                    Console.WriteLine(ex.Message); 
                    allOK = false;

                }

            }
            else
            {
                return shObjList;
            }
          
            return shObjList;

        }

        public bool WriteWarmCandles(CandleResponceWithFigiUID candleObjWithFigi, string connString, DateTime insertDate)
        {
            Boolean allOK = true;
            int localTotalCounter = 0;
            int localWritedCounter = 0;
            int localNotNeedWritCounter = 0;


            using var connection = new NpgsqlConnection(connString);
            try
            {
                connection.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                allOK = false;
            }

            if (allOK)
            {
                localTotalCounter = candleObjWithFigi.CandlesResponse.Candles.Count;
                foreach (var candle in candleObjWithFigi.CandlesResponse.Candles)
                {
                        try
                        {
                            var dBRequest = "insert into tmp_warm_history_candles (figi, candle_start_dt, open_price, close_price, max_price, min_price, volume, source_filename, insertdate, guidfromfile, source,is_close_candle) values (@figi, @candle_start_dt, @open_price, @close_price, @max_price, @min_price, @volume, @source_filename, @insertdate, @guidfromfile, @source,@is_close_candle)";
                            using var command = new NpgsqlCommand(dBRequest, connection);
                            command.Parameters.AddWithValue("figi", candleObjWithFigi.Figi);
                            command.Parameters.AddWithValue("candle_start_dt", candle.Time.ToDateTime());
                            command.Parameters.AddWithValue("open_price", float.Parse(candle.Open.Units.ToString() + "," + candle.Open.Nano.ToString()));
                            command.Parameters.AddWithValue("close_price", float.Parse(candle.Close.Units.ToString() + "," + candle.Close.Nano.ToString()));
                            command.Parameters.AddWithValue("max_price", float.Parse(candle.High.Units.ToString() + "," + candle.High.Nano.ToString()));
                            command.Parameters.AddWithValue("min_price", float.Parse(candle.Low.Units.ToString() + "," + candle.Low.Nano.ToString()));
                            command.Parameters.AddWithValue("volume", candle.Volume);
                            command.Parameters.AddWithValue("source_filename", "no_file");
                            command.Parameters.AddWithValue("insertdate", insertDate.ToUniversalTime());
                            command.Parameters.AddWithValue("guidfromfile", candleObjWithFigi.Uid);
                            command.Parameters.AddWithValue("source", "GetWarmCandlesApp");
                            command.Parameters.AddWithValue("is_close_candle", candle.IsComplete);
                            command.Prepare();
                            command.ExecuteNonQuery();
                            localWritedCounter++;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.ToString());
                        }
                }
            }
            else
            {
                return allOK;
            }
            if (localTotalCounter > 1)
            {
                Console.WriteLine("figi " + candleObjWithFigi.Figi);
                Console.WriteLine("Всего свече: " + localTotalCounter);
                Console.WriteLine("Свечей записано: " + localWritedCounter);

            }

            return allOK;


        }

        public int fromTmpWarm2PromWarm(string connString)
        {
            int result = 0;

            string dBRequest = @"insert into warm_history_candles select t1.* from tmp_warm_history_candles t1 left join warm_history_candles t2 on t1.figi = t2.figi and t1.candle_start_dt = t2.candle_start_dt where t2.figi is null";
            Boolean allOK = true;

            using var connection = new NpgsqlConnection(connString);
            try
            {
                connection.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                allOK = false;
            }
            if (allOK)
            { 
                try
                {
                    using var command = new NpgsqlCommand(dBRequest, connection);
                    result = command.ExecuteNonQuery();

                }
                catch (Exception ex)
                {

                    Console.WriteLine(ex.Message);
                    allOK = false;

                }
            }
            else
            { 
                return result;
            }


            return result;
        }

        public object UpdateNotClosedCandles(string connString)
        {
            int result = 0;

            string dBRequest = @"update warm_history_candles ut1 set open_price = sub_t2.open_price,close_price = sub_t2.close_price,max_price = sub_t2.max_price,min_price = sub_t2.min_price,volume = sub_t2.volume,insertdate = sub_t2.insertdate,source = sub_t2.source,is_close_candle = sub_t2.is_close_candle from (select t2.* from warm_history_candles t1
         left join tmp_warm_history_candles t2
             on t1.figi = t2.figi
                    and t1.candle_start_dt = t2.candle_start_dt
                    and t2.is_close_candle = true
          where t1.is_close_candle = false and t2.figi is not null) as sub_t2
where ut1.figi = sub_t2.figi and ut1.candle_start_dt = sub_t2.candle_start_dt;

delete from tmp_warm_history_candles where 1=1;";
            Boolean allOK = true;

            using var connection = new NpgsqlConnection(connString);
            try
            {
                connection.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                allOK = false;
            }
            if (allOK)
            {
                try
                {
                    using var command = new NpgsqlCommand(dBRequest, connection);
                    result = command.ExecuteNonQuery();

                }
                catch (Exception ex)
                {

                    Console.WriteLine(ex.Message);
                    allOK = false;

                }
            }
            else
            {
                return result;
            }


            return result;
        }

        public bool CleanUpTable(string connString)
        {
            bool result = false;

            string dBRequest = @"delete from tmp_warm_history_candles where 1=1;";
            Boolean allOK = true;

            using var connection = new NpgsqlConnection(connString);
            try
            {
                connection.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                allOK = false;
            }
            if (allOK)
            {
                try
                {
                    using var command = new NpgsqlCommand(dBRequest, connection);
                    command.ExecuteNonQuery();
                    result = true;
                }
                catch (Exception ex)
                {

                    Console.WriteLine(ex.Message);
                    allOK = false;
                    result= false;

                }
            }
            else
            {
                return result;
            }


            return result;
        }
    }
}
