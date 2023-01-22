using FinInvestLibrary.Functions;
using FinInvestLibrary.Objects;
using FinInvestLibrary.Objects.Logging;
using Google.Protobuf.WellKnownTypes;
using Npgsql;
using Tinkoff.InvestApi;
using Tinkoff.InvestApi.V1;

namespace GetWarmCandles
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var answer = FinBaseDbFunctions.AddLogRowToLogTable(new LogObject(), "str");
            //Токен доступа к Тинькофф
            string token = "t.hrRraHICLaGVw1xOFtzsF2WZHQ5tFZ8G9M5AAlJd9e54Yhe3kkygVSfWVyk2IZGae_-ENntIv_pK_f7C4hqw8g";
            string connectionString = "Host=localhost;Username=postgres;Password=#6TY0N0d;Database=FinBase";
            int requesTimeOutInterval = 0; //in milliseconds

            int searchDayDeep = 1;
            DateTime currentDatetime = DateTime.Now;
            //Получаем список часов и дат на интервале searchDayDeep - позже используем для сверки с реально существующими.
            List<DateTime> ethalonDatesList = GetDateList(DateTime.Now, currentDatetime.AddDays(-searchDayDeep));

            List<ShareObject> shareObjects = GetSharesFromDB(connectionString);

            int innerCounter = 0;

            //Определяем какое кол-во свечей нужно загрузить для каждого инструмента и формируем список объектов candlesRequest для запроса истории свечей
            List<GetCandlesRequest> candlesRequests = GetCandlesRequestObjects(connectionString, searchDayDeep, ethalonDatesList, shareObjects);

            //Запрашиваем Тинькофф с целью получить свечи за указанные в запросе периоды
            List<CandleResponceWithFigiUID> candleListWithFigi = GetCandlesResponseObjects(requesTimeOutInterval,token, candlesRequests);



            //Записываем полученный результат в таблицу Warm_History_Candles
            DateTime warmInsertStart = DateTime.Now;
            foreach (var candleCollection in candleListWithFigi)
            {
                DateTime insertDateTime = DateTime.UtcNow;
                bool res = WriteWarmCandles(candleCollection, connectionString, insertDateTime);
            }
            DateTime warmInsertFinish = DateTime.Now;
            Console.WriteLine((warmInsertFinish - warmInsertStart).TotalSeconds / 60);



        }

        private static bool WriteWarmCandles(CandleResponceWithFigiUID candleObjWithFigi, string connString, DateTime insertDate)
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
                    
                    bool isHavingEqualData = checkForHavingData(connString, candleObjWithFigi, candle);
                    if (isHavingEqualData is false)
                    {
                        
                        try
                        {
                            var dBRequest = "insert into warm_history_candles (figi, candle_start_dt, open_price, close_price, max_price, min_price, volume, source_filename, insertdate, guidfromfile, source,is_close_candle) values (@figi, @candle_start_dt, @open_price, @close_price, @max_price, @min_price, @volume, @source_filename, @insertdate, @guidfromfile, @source,@is_close_candle)";
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
                    else
                        localNotNeedWritCounter++;
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
                Console.WriteLine("Свечей не требует записи: " + localNotNeedWritCounter);
            }

            return allOK;
           
           
        }

        private static bool checkForHavingData(string connString, CandleResponceWithFigiUID candleResponce, HistoricCandle candle)
        {
            int localCounter = 0;
            bool isHavingCandle = false;
            using var connection = new NpgsqlConnection(connString);
            try
            {
                connection.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            try
            {
                //'2023-01-21 15:00:00'
                var dBRequest = "SELECT count(*) FROM warm_history_candles where figi = '" + candleResponce.Figi + "' and candle_start_dt = '" + candle.Time.ToDateTime().ToString("yyyy-MM-dd HH:mm") + ":00'";
                Console.WriteLine(dBRequest);

                using var command = new NpgsqlCommand(dBRequest, connection);
                var reader = command.ExecuteScalar();
                
                localCounter = Convert.ToInt32(reader.ToString());

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }

            if (localCounter > 0)
                isHavingCandle = true;
            else
                isHavingCandle = false;

            Console.WriteLine(isHavingCandle);

            return isHavingCandle;
        }

        /// <summary>
        /// Запрос Тинькофф с целью получения истории свечей
        /// </summary>
        /// <param name="token">Токен доступа к Tinkoff API</param>
        /// <param name="candlesRequests">Список подготовленных запросов к Tinkoff API</param>
        /// <returns></returns>
        private static List<CandleResponceWithFigiUID> GetCandlesResponseObjects(int requesTimeOutInterval,string token, List<GetCandlesRequest> candlesRequests)
        {
            var client = InvestApiClientFactory.Create(token);

            var candleList = new List<CandleResponceWithFigiUID>();
            int counter = 0;
            var at = DateTime.Now;
            foreach (var candleReq in candlesRequests)
            {
                counter++;
                var a = DateTime.Now;
                try
                {
                    var response = client.MarketData.GetCandles(candleReq);
                    CandleResponceWithFigiUID cRWFUID = new CandleResponceWithFigiUID();
                    cRWFUID.Figi = candleReq.Figi;
                    cRWFUID.Uid = candleReq.InstrumentId;
                    cRWFUID.CandlesResponse = response;
                    candleList.Add(cRWFUID);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
                var b = DateTime.Now;
                Console.WriteLine(counter + "\t" + (b - a).TotalMilliseconds + "\t" + candleReq.Figi);
                Thread.Sleep(requesTimeOutInterval);
            }
            client = null;
            var bt = DateTime.Now;
            Console.WriteLine((bt - at).TotalSeconds / 60);
            return candleList;
        }

        /// <summary>
        /// Формируем список запросов к Tinkoff API для получения истории по свечам выбранных инструментов
        /// </summary>
        /// <param name="connectionString">строка подключения к БД FinBase</param>
        /// <param name="searchDayDeep">глубина запроса на поиск пропусков в свечных данных, сохраненных ранее</param>
        /// <param name="ethalonDatesList">обязательный список дат, которые должны быть в таблице warm_history_candles</param>
        /// <param name="shareObjects">список инструментов для формирования запросов свечей к Tinkoff API</param>
        /// <returns></returns>
        private static List<GetCandlesRequest> GetCandlesRequestObjects(string connectionString, int searchDayDeep, List<DateTime> ethalonDatesList, List<ShareObject> shareObjects)
        {
            DateTime currentDatetime = DateTime.Now;
            List<GetCandlesRequest> candlesRequests = new List<GetCandlesRequest>();
            foreach (var share in shareObjects)
            {
                var a = DateTime.Now;
                //Получаем список часов и дат по которым есть информация по свечам
                List<DateTime> hours = GetNeededHoursForDownloadCandles(connectionString, share, searchDayDeep);

                List<DateTime> notFoundDateList = getNotFoundDateList(ethalonDatesList, hours);
                print("Для инструмента " + share.name + " требуется прогрузка " + notFoundDateList.Count + " свечей");

                GetCandlesRequest request = new GetCandlesRequest();
                request.Figi = share.figi;
                request.From = Timestamp.FromDateTime(notFoundDateList.OrderBy(o => o.Ticks).First().ToUniversalTime());
                request.To = Timestamp.FromDateTime(currentDatetime.AddMinutes(-currentDatetime.Minute).AddSeconds(-currentDatetime.Second).AddMilliseconds(-currentDatetime.Millisecond).ToUniversalTime());
                request.Interval = CandleInterval._15Min;
                request.InstrumentId = share.uid;

                candlesRequests.Add(request);


            }

            return candlesRequests;
        }

        /// <summary>
        /// Получаем перечень пропущенных дат для инструмента
        /// </summary>
        /// <param name="ethalonDatesList">перечень эталонных дат</param>
        /// <param name="hours"></param>
        /// <returns></returns>
        private static List<DateTime> getNotFoundDateList(List<DateTime> ethalonDatesList, List<DateTime> hours)
        {
            List<DateTime> outputDateList = new List<DateTime>();
            foreach (var ethDay in ethalonDatesList)
            {
                bool findResult = false;

                var eYear = ethDay.Year;
                var eMonth = ethDay.Month;
                var eDay = ethDay.Day;
                var eHour = ethDay.Hour;

                foreach (var findDate in hours)
                {
                    var year = findDate.Year;
                    var month = findDate.Month;
                    var day = findDate.Day;
                    var hour = findDate.Hour;

                    bool bYear = (eYear == year);
                    bool bMonth = (eMonth == month);
                    bool bDay = (eDay == day);
                    bool bHour = (eHour == hour);

                    if (bYear && bMonth && bDay && bHour)
                    {
                        findResult = true;
                        break;
                    }
                }
                if (findResult is false)
                {
                    outputDateList.Add(ethDay);
                }
            }
            return outputDateList;
        }

        /// <summary>
        /// Создаем эталон списка дат для проверки наличия данных в БД по инструментам
        /// </summary>
        /// <param name="now"></param>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        private static List<DateTime> GetDateList(DateTime nowDatetime, DateTime pastDateTime)
        {

            pastDateTime = pastDateTime.AddMinutes(-pastDateTime.Minute).AddSeconds(-pastDateTime.Second).AddMilliseconds(-pastDateTime.Millisecond);
            List<DateTime> res = new List<DateTime>();
            while (pastDateTime <= nowDatetime)
            {
                res.Add(pastDateTime);
                pastDateTime = pastDateTime.AddHours(1);
            }
            return res;

        }

        /// <summary>
        /// Проверяем какие периоды за последние 3 дня у нас не заполнены по инструментам
        /// </summary>
        /// <param name="connectionString"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        private static List<DateTime> GetNeededHoursForDownloadCandles(string connString, ShareObject share, int searchDayDeep)
        {
            List<DateTime> dateTimeList = new List<DateTime>();

            //print("Получаю пропуски в данных из warm_history_candles");
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
                var dBRequest = "select date_trunc('hour',candle_start_dt), count(*) from warm_history_candles where figi = '" + share.figi + "' and  candle_start_dt > now()::DATE - " + searchDayDeep + " group by date_trunc('hour',candle_start_dt);";
                try
                {
                    using var command = new NpgsqlCommand(dBRequest, connection);
                    using NpgsqlDataReader reader = command.ExecuteReader();
                    while (reader.Read())
                    {
                        DateTime dateTime = reader.GetDateTime(0);
                        dateTimeList.Add(dateTime);
                    }
                }
                catch (Exception ex)
                {
                    print("В GetNeededHoursForDownloadCandles возникла ошибка", true);
                    print(ex.ToString(), true);
                    allOK = false;

                }

            }
            else
            {
                return dateTimeList;
            }



            //print("БД: Получено " + dateTimeList.Count + " дат.");

            return dateTimeList;
        }

        /// <summary>
        /// Получаем перечень акций в виде списка объектов SharesObject
        /// </summary>
        /// <param name="connString"></param>
        /// <returns></returns>
        private static List<ShareObject> GetSharesFromDB(string connString)
        {
            print("Получаю список акций из БД");
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
                    print("В GetSharesFromDB возникла ошибка", true);
                    print(ex.ToString(), true);
                    allOK = false;

                }

            }
            else
            {
                return shObjList;
            }
            print("БД: Получено " + shObjList.Count + " инструментов.");
            return shObjList;

        }


        #region Logging
        private static void print(string message, bool isError)
        {
            if (isError)
                Console.WriteLine("{0} ERROR: {1}", DateTime.Now, message);
            else
                Console.WriteLine("{0} INFO: {1}", DateTime.Now, message);
        }
        private static void print(string message)
        {
            Console.WriteLine("{0} INFO: {1}", DateTime.Now, message);
        }
        #endregion
    }
}