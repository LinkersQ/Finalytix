using FinInvestLibrary.Objects;
using Google.Protobuf.WellKnownTypes;
using Tinkoff.InvestApi;
using Tinkoff.InvestApi.V1;

namespace FinInvestLibrary.Functions.LocalOperations
{
    public class TinkoffInvestApiFunctions
    {

        /// <summary>
        /// Формируем список запросов к Tinkoff API для получения истории по свечам выбранных инструментов
        /// </summary>
        /// <param name="shareObjects">Список инструментов по которым требуется сформировать запрос</param>
        /// <param name="currentDateTime">Дата и время окончания интервала</param>
        /// <param name="searchDayDeep">Глубина запроса на поиск пропусков в свечных данных, сохраненных ранее</param>
        /// <returns>Возвращает список объектов GetCandlesRequest</returns>
        public List<GetCandlesRequest> GetCandlesRequestObjects(List<ShareObject> shareObjects, DateTime currentDateTime, int searchDayDeep)
        {
            DateTime currentDatetime = DateTime.Now;
            List<GetCandlesRequest> candlesRequests = new List<GetCandlesRequest>();
            foreach (var share in shareObjects)
            {
                GetCandlesRequest request = new GetCandlesRequest();
                request.Figi = share.figi;
                request.From = Timestamp.FromDateTime(currentDateTime.AddDays(-searchDayDeep));
                request.To = Timestamp.FromDateTime(currentDateTime);
                request.Interval = CandleInterval.Day;
                request.InstrumentId = share.uid;
                candlesRequests.Add(request);
            }

            return candlesRequests;
        }

        /// <summary>
        /// Запрос Тинькофф с целью получения истории свечей
        /// </summary>
        /// <param name="requesTimeOutInterval">Период задержки чтобы тинькофф не отрубил по перелимиту</param>
        /// <param name="token">Токен доступа к Tinkoff API</param>
        /// <param name="candlesRequests">Список подготовленных запросов к Tinkoff API</param>
        /// <returns></returns>
        public List<CandleResponceWithFigiUID> GetCandlesResponseObjects(int requesTimeOutInterval, string token, List<GetCandlesRequest> candlesRequests)
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
    }
}
