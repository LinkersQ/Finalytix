using FinInvestLibrary.Objects;
using FinInvestLibrary.Objects.Logging;
using FinInvestLibrary.Functions.LocalOperations;
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
            string token = "t.hrRraHICLaGVw1xOFtzsF2WZHQ5tFZ8G9M5AAlJd9e54Yhe3kkygVSfWVyk2IZGae_-ENntIv_pK_f7C4hqw8g";
            string connectionString = "Host=localhost;Username=postgres;Password=#6TY0N0d;Database=FinBase";
            int requestTimeOutInterval = 200; //для ограничений кол-ва запросов к тинькофф апи
            DateTime currentDateTime = DateTime.UtcNow; //Tinkoff API работает всегда в UTC - придерживаемся тоже UTC;
            int searchPeriod = 24;//глубина поиска при запросе свечей

            Console.WriteLine("Запущен процесс получения свежих данных о свечах от TinkoffAPI");

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
            Console.WriteLine("Запрашиваю TinkoffAPI и получаю свечи");
            var responses = tinkoffInvestApiFunctions.GetCandlesResponseObjects(requestTimeOutInterval, token, requests);
            Console.WriteLine("Получено {0} свечей по {1} инструментам", responses.Count, responses.Sum(c => c.CandlesResponse.Candles.Count));

            //Сохраняем полученные свечи в таблицу tmp_warm_history_candles
            Console.WriteLine("Приступаю к записи полученных данных в таблицу tmp_warm_history_candles");
            PutCandlesIntoDB(connectionString, currentDateTime, dBConnector, responses);
            Console.WriteLine("Полученные свечи успешно записаны");

            //Перекладываем свечи в рабочую таблицу, обновляем ранее полученные но не закрытые свечи, очищаем временную таблицу.
            Console.WriteLine("Перемещаю недостающие свечи");
            var result_tmp2Warm = dBConnector.fromTmpWarm2PromWarm(connectionString);
            Console.WriteLine("Перемещено {0} свечей", result_tmp2Warm);

            Console.WriteLine("Обновляю ранее незакрытые свечи");
            var result_UpdNotClosedCandles = dBConnector.UpdateNotClosedCandles(connectionString);
            Console.WriteLine("Обновлено {0} свечей", result_UpdNotClosedCandles);

            Console.WriteLine("Очищаем временную таблицу");
            var result_CleanUpTable = dBConnector.CleanUpTable(connectionString);
            if (result_CleanUpTable)
                Console.WriteLine("Таблица успешно очищена {0}", result_CleanUpTable);
            else
                Console.WriteLine("Не удалось очистить таблицу");


            #endregion

            var finishTime = DateTime.UtcNow;
            Console.WriteLine("Затрачено времени: {0} минут", (finishTime-currentDateTime).TotalSeconds/60);


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
                    Console.WriteLine("При записи figi {0} возникли ошибки.");
            }
            
        }

        private static List<GetCandlesRequest> GetRequests(DateTime currentDateTime, int searchPeriod, TinkoffInvestApiFunctions tinkoffInvestApiFunctions, List<ShareObject> shares)
        {
            Console.WriteLine("Подготавливаю запросы в TinkoffAPI для каждого инструмента");
            Console.WriteLine("\tВводные данные для запросов");
            Console.WriteLine("\t\tИнтервал:");
            Console.WriteLine("\t\t\tНачало:{0}", currentDateTime.ToString());
            Console.WriteLine("\t\t\tГлубина поиска: {0}", searchPeriod);
            var requests = tinkoffInvestApiFunctions.GetCandlesRequestObjects(shares, currentDateTime, searchPeriod);
            Console.WriteLine("По " + shares.Count + " инструментам сформировано " + requests.Count + " запросов");
            return requests;
        }

        private static List<ShareObject> GetShares(string connectionString, FinBaseConnector dBConnector)
        {
            Console.WriteLine("Получаю список инструментов для которых нужно обновить свечи");
            var shares = dBConnector.GetSharesFromDB(connectionString);
            Console.WriteLine("Получено " + shares.Count + " инструментов");
            return shares;
        }
    }
}