using FinInvestLibrary.Objects;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GetWarmCandles.LocalOperations
{
    public class FinBaseConnector
    {
        /// <summary>
        /// Запрос всех инструментов, которые есть в хранилище Shares
        /// </summary>
        /// <param name="connString">Передаем строку подключения</param>
        /// <returns>Возвращает сисок ShareObject</returns>
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
    }
}
