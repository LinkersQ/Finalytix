using FinInvestLibrary.Objects;
using FinInvestLibrary.Objects.Logging;
using FinInvestLibrary.Functions.LocalOperations;
using Google.Protobuf.WellKnownTypes;
using Npgsql;
using Tinkoff.InvestApi;
using Tinkoff.InvestApi.V1;
using log4net;
using log4net.Config;

namespace GetWarmCandles
{
    static class Program
    {
        public static readonly ILog log = LogManager.GetLogger(typeof(Program));
        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();
            log.Info("---Start---");
            try
            {
                
                //string token = "t.hrRraHICLaGVw1xOFtzsF2WZHQ5tFZ8G9M5AAlJd9e54Yhe3kkygVSfWVyk2IZGae_-ENntIv_pK_f7C4hqw8g";
                //string connectionString = "Host=localhost;Username=postgres;Password=#6TY0N0d;Database=FinBase";
                int requestTimeOutInterval = 200; //для ограничений кол-ва запросов к тинькофф апи
                DateTime currentDateTime = DateTime.UtcNow; //Tinkoff API работает всегда в UTC - придерживаемся тоже UTC;
                int searchPeriod = 24;//глубина поиска при запросе свечей
                string appPath = Environment.CurrentDirectory;
                string connectionStringPath = appPath + "\\connectionString.txt";
                string connectionString = File.ReadAllText(connectionStringPath);
                string tokenPath = appPath + "\\token.txt";
                string token = File.ReadAllText(tokenPath);

              
                log.Info("Запущен процесс получения свежих данных о свечах от TinkoffAPI");

                FinBaseConnector dBConnector = new FinBaseConnector();//Инициируем коннектор к базе данных FinBase. Используется для всех операций с БД
                TinkoffInvestApiFunctions tinkoffInvestApiFunctions = new TinkoffInvestApiFunctions();//Инициируем набор функций для работы с тинькофф АПИ

                #region Подготавливаем запросы к TinkoffAPI - формируем объекты GetCandlesRequest

                //Сначала получаем список инструментов для которых нужно получить свечи
                List<ShareObject> shares = GetShares(connectionString, dBConnector);


                //Теперь формируем запрос GetCandlesRequest для каждого инструментафо
                List<GetCandlesRequest> requests = GetRequests(currentDateTime, searchPeriod, tinkoffInvestApiFunctions, shares);

                #endregion
                #region Получаем свечи от тинькофф и записываем в таблицу
                //Запускаем процесс получения данных по свечам от TinkoffAPI
                
                log.Info("Запрашиваю TinkoffAPI и получаю свечи");
                var responses = tinkoffInvestApiFunctions.GetCandlesResponseObjects(requestTimeOutInterval, token, requests);
                
                log.Info("Получено " + responses.Count + " свечей по " + responses.Sum(c => c.CandlesResponse.Candles.Count) + " инструментам");

                //Сохраняем полученные свечи в таблицу tmp_warm_history_candles
                
                log.Info("Приступаю к записи полученных данных в таблицу tmp_warm_history_candles");
                PutCandlesIntoDB(connectionString, currentDateTime, dBConnector, responses);
                
                log.Info("Полученные свечи успешно записаны");

                //Перекладываем свечи в рабочую таблицу, обновляем ранее полученные но не закрытые свечи, очищаем временную таблицу.
                
                log.Info("Перемещаю недостающие свечи");
                var result_tmp2Warm = dBConnector.fromTmpWarm2PromWarm(connectionString);
                
                log.Info("Перемещено " + result_tmp2Warm + " свечей");

                log.Info("Обновляю ранее незакрытые свечи");
                var result_UpdNotClosedCandles = dBConnector.UpdateNotClosedCandles(connectionString);
               
                log.Info("Обновлено " + result_UpdNotClosedCandles + " свечей");

              
                log.Info("Очищаем временную таблицу");
                var result_CleanUpTable = dBConnector.CleanUpTable(connectionString);
                if (result_CleanUpTable)
                {
                   
                    log.Info("Таблица успешно очищена " + result_CleanUpTable);
                }
                else
                {
                   
                    log.Info("Не удалось очистить таблицу");
                }


                #endregion

                var finishTime = DateTime.UtcNow;
                
                log.Info("Затрачено времени: " + (finishTime - currentDateTime).TotalSeconds / 60 + " минут");
            }
            catch (Exception ex)
            {
                log.Error(ex.ToString());
            }
            log.Info("---End---");
        }

        private static void PutCandlesIntoDB(string connectionString, DateTime currentDateTime, FinBaseConnector dBConnector, List<CandleResponceWithFigiUID> responses)
        {
            
            foreach (var response in responses)
            {
                Console.WriteLine("Отправляю на запись figi {0}. Нужно записать {1} свечей...", response.Figi, response.CandlesResponse.Candles.Count);
                var result = dBConnector.WriteWarmCandles(response, connectionString, currentDateTime);
                if (result)
                    Console.WriteLine("Свечи для figi {0} успешно записаны.", response.Figi);
                else
                {
                    Console.WriteLine("При записи figi {0} возникли ошибки.");
                    log.Error("При записи figi " + response.Figi + " возникли ошибки.");
                }
            }
            
        }

        private static List<GetCandlesRequest> GetRequests(DateTime currentDateTime, int searchPeriod, TinkoffInvestApiFunctions tinkoffInvestApiFunctions, List<ShareObject> shares)
        {
            Console.WriteLine("Подготавливаю запросы в TinkoffAPI для каждого инструмента");
            Console.WriteLine("\tВводные данные для запросов");
            Console.WriteLine("\t\tИнтервал:");
            Console.WriteLine("\t\t\tНачало:{0}", currentDateTime.ToString());
            Console.WriteLine("\t\t\tГлубина поиска: {0}", searchPeriod);
            List<GetCandlesRequest> requests = new List<GetCandlesRequest>();
            try
            {
                 requests = tinkoffInvestApiFunctions.GetCandlesRequestObjects(shares, currentDateTime, searchPeriod);
                 Console.WriteLine("По " + shares.Count + " инструментам сформировано " + requests.Count + " запросов");
                
            }
            catch (Exception ex)
            {
                log.Error("Во время формирования списка заппросов к TinkoffAPI возгникла ошибка");
                log.Error(ex.ToString());
            }
            return requests;

        }

        private static List<ShareObject> GetShares(string connectionString, FinBaseConnector dBConnector)
        {
            Console.WriteLine("Получаю список инструментов для которых нужно обновить свечи");
            List<ShareObject> shares = new List<ShareObject>();
            try
            {
                shares = dBConnector.GetSharesFromDB(connectionString);
            }
            catch (Exception ex)
            {
                log.Error("Во время получения списка инструментов возникли ошибки");
                log.Error(ex.ToString());
            }
            Console.WriteLine("Получено " + shares.Count + " инструментов");
            return shares;
        }
    }
}