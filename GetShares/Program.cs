using FinInvestLibrary.Objects;
using Npgsql;
using System.Globalization;
using Tinkoff.InvestApi;
using Tinkoff.InvestApi.V1;

namespace GetShares
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            print("Запуск", false);
            //Переменные
            //string token = "t.hrRraHICLaGVw1xOFtzsF2WZHQ5tFZ8G9M5AAlJd9e54Yhe3kkygVSfWVyk2IZGae_-ENntIv_pK_f7C4hqw8g";
            //string connectionString = "Host=localhost;Username=postgres;Password=#6TY0N0d;Database=FinBase";
            int countOfAddedShares = 0;

            string appPath = Environment.CurrentDirectory;
            string connectionStringPath = appPath + "\\connectionString.txt";
            string connectionString = File.ReadAllText(connectionStringPath);
            string tokenPath = appPath + "\\token.txt";
            string token = File.ReadAllText(tokenPath);


            //Исполнение
            SharesResponse shares = GetSharesFromTinkoffInvestApi(token);
            List<ShareObject> tinkoffSharesList = GenerateSharesObjects(shares);
            List<ShareObject> dbSharesList = GetSharesFromDB(connectionString);
            List<ShareObject> sharesToAddList = null;

            //Если получен список акций из БД проводим сверку с акциями, полученными из Тинькофф
            if (dbSharesList is not null)
            {
                sharesToAddList = EqualSharesLists(tinkoffSharesList, dbSharesList);
                countOfAddedShares = AddSharesToDB(connectionString, sharesToAddList);
            }
            //Если в бд нет данных - просто записываем все что получили от Тинькофф в таблицу
            else
            {
                countOfAddedShares = AddSharesToDB(connectionString, tinkoffSharesList);
            }


        }

        private static List<ShareObject> EqualSharesLists(List<ShareObject> tinkoffSharesList, List<ShareObject> dbSharesList)
        {
            print("Сравниваю полученные списки");
            List<ShareObject> returnShareList = new List<ShareObject>();

            foreach (var tinkoffShareObj in tinkoffSharesList)
            {
                ShareObject objToAdd = null;
                objToAdd = dbSharesList.Find(a => a.figi == tinkoffShareObj.figi); //пробуем найти совпадение в БД
                if (objToAdd is null) //если не находим
                {
                    objToAdd = tinkoffShareObj; // присваиваем не найденый объект объекту для инсерта в БД
                    returnShareList.Add(objToAdd);
                    print("Не найден Figi: " + objToAdd.figi);
                }
            }

            if (returnShareList.Count == 0)
                print("Нет акций, которые требуется добавить");
            else
                print("Требуется добавить " + returnShareList.Count + " акций.");

            return returnShareList;
        }

        //Вывод лога в консоль
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

        //Вставляем акции в БД 
        private static int AddSharesToDB(string connString, List<ShareObject> sharesToAddList)
        {
            Boolean allOK = true;
            int counter = 0;

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
                foreach (var shareObj in sharesToAddList)
                {
                    var dBRequest = "INSERT INTO public.Shares (figi, ticker, class_code, isin, lot, currency, short_enabled_flag, name, exchange, issue_size, country_of_risk, country_of_risk_name, sector, issue_size_plan, trading_status, otc_flag, buy_available_flag, sell_available_flag, div_yield_flag, share_type, min_price_increment, api_trade_available_flag, uid, real_exchange, position_uid, for_iis_flag, for_qual_investor_flag, weekend_flag, blocked_tca_flag) VALUES(@figi, @ticker, @class_code, @isin, @lot, @currency, @short_enabled_flag, @name, @exchange, @issue_size, @country_of_risk, @country_of_risk_name, @sector, @issue_size_plan, @trading_status, @otc_flag, @buy_available_flag, @sell_available_flag, @div_yield_flag, @share_type, @min_price_increment, @api_trade_available_flag, @uid, @real_exchange, @position_uid, @for_iis_flag, @for_qual_investor_flag, @weekend_flag, @blocked_tca_flag)";
                    try
                    {
                        using var command = new NpgsqlCommand(dBRequest, connection);
                        command.Parameters.AddWithValue("figi", shareObj.figi);
                        command.Parameters.AddWithValue("ticker", shareObj.ticker);
                        command.Parameters.AddWithValue("class_code", shareObj.class_code);
                        command.Parameters.AddWithValue("isin", shareObj.isin);
                        command.Parameters.AddWithValue("lot", shareObj.lot);
                        command.Parameters.AddWithValue("currency", shareObj.currency);
                        command.Parameters.AddWithValue("short_enabled_flag", shareObj.short_enabled_flag);
                        command.Parameters.AddWithValue("name", shareObj.name);
                        command.Parameters.AddWithValue("exchange", shareObj.exchange);
                        command.Parameters.AddWithValue("issue_size", shareObj.issue_size);
                        command.Parameters.AddWithValue("country_of_risk", shareObj.country_of_risk);
                        command.Parameters.AddWithValue("country_of_risk_name", shareObj.country_of_risk_name);
                        command.Parameters.AddWithValue("sector", shareObj.sector);
                        command.Parameters.AddWithValue("issue_size_plan", shareObj.issue_size_plan);
                        command.Parameters.AddWithValue("trading_status", shareObj.trading_status);
                        command.Parameters.AddWithValue("otc_flag", shareObj.otc_flag);
                        command.Parameters.AddWithValue("buy_available_flag", shareObj.buy_available_flag);
                        command.Parameters.AddWithValue("sell_available_flag", shareObj.sell_available_flag);
                        command.Parameters.AddWithValue("div_yield_flag", shareObj.div_yield_flag);
                        command.Parameters.AddWithValue("share_type", shareObj.share_type);
                        command.Parameters.AddWithValue("min_price_increment", shareObj.min_price_increment);
                        command.Parameters.AddWithValue("api_trade_available_flag", shareObj.api_trade_available_flag);
                        command.Parameters.AddWithValue("uid", shareObj.uid);
                        command.Parameters.AddWithValue("real_exchange", shareObj.real_exchange);
                        command.Parameters.AddWithValue("position_uid", shareObj.position_uid);
                        command.Parameters.AddWithValue("for_iis_flag", shareObj.for_iis_flag);
                        command.Parameters.AddWithValue("for_qual_investor_flag", shareObj.for_qual_investor_flag);
                        command.Parameters.AddWithValue("weekend_flag", shareObj.weekend_flag);
                        command.Parameters.AddWithValue("blocked_tca_flag", shareObj.blocked_tca_flag);
                        command.Prepare();
                        command.ExecuteNonQuery();

                        print(shareObj.figi + " is inserted");
                        counter++;

                    }

                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                        allOK = false;

                    }
                }

            }
            else
            {
                return counter;
            }




            return counter;
        }

        //Получаем актуальный справочник акций из БД и подготавливаем список акций для сверки
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

        //Передаем список полученных акций из GetSharesFromTinkoffInvestApi и формируем список объектов ShareObject (список акций для сверки с базой данных)
        private static List<ShareObject> GenerateSharesObjects(SharesResponse shares)
        {
            List<ShareObject> _shareList = new List<ShareObject>();
            if (shares is not null)
            {
                if (shares.Instruments.Count > 0)
                {
                    foreach (var share in shares.Instruments)
                    {
                        ShareObject shObj = new ShareObject
                        {
                            figi = share.Figi,
                            ticker = share.Ticker,
                            class_code = share.ClassCode,
                            isin = share.Isin,
                            lot = share.Lot,
                            currency = share.Currency,
                            short_enabled_flag = share.ShortEnabledFlag,
                            name = share.Name,
                            exchange = share.Exchange,
                            issue_size = share.IssueSize,
                            country_of_risk = share.CountryOfRisk,
                            country_of_risk_name = share.CountryOfRiskName,
                            sector = share.Sector,
                            issue_size_plan = share.IssueSizePlan,
                            trading_status = share.TradingStatus.ToString(),
                            otc_flag = share.OtcFlag,
                            buy_available_flag = share.BuyAvailableFlag,
                            sell_available_flag = share.SellAvailableFlag,
                            div_yield_flag = share.DivYieldFlag,
                            share_type = share.ShareType.ToString(),
                            min_price_increment = float.Parse($"{share.MinPriceIncrement.Units}.{share.MinPriceIncrement.Nano}", CultureInfo.InvariantCulture.NumberFormat),
                            api_trade_available_flag = share.ApiTradeAvailableFlag,
                            uid = share.Uid,
                            real_exchange = share.RealExchange.ToString(),
                            position_uid = share.PositionUid,
                            for_iis_flag = share.ForIisFlag,
                            for_qual_investor_flag = share.ForQualInvestorFlag,
                            weekend_flag = share.WeekendFlag,
                            blocked_tca_flag = share.BlockedTcaFlag
                        };
                        _shareList.Add(shObj);
                    }
                }
                else
                {
                    print("Нет акций для записи");
                }
            }
            else
            {
                print("Не удалось получить список акций");
            }
            print("Тинькофф: Получено " + _shareList.Count + " инструментов.");
            return _shareList;
        }

        //Подключаемся к Тинькофф и забираем список акций
        private static SharesResponse GetSharesFromTinkoffInvestApi(string token)
        {
            print("Получаю список акций у Тинькофф");
            var client = InvestApiClientFactory.Create(token);
            SharesResponse shares = null;
            try
            {
                shares = client.Instruments.Shares();
            }
            catch (Exception ex)
            {
                print("В SharesResponse возникла ошибка", true);
                print(ex.ToString(), true);
            }

            return shares;
        }
    }
}