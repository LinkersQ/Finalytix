using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using System.Text.Json;
using Tinkoff.InvestApi;
using Tinkoff.InvestApi.V1;

namespace TCSStreamReader
{
    internal class TCSStreamReaderClass
    {
        private static void Main(string[] args)
        {
            var token = "t.hrRraHICLaGVw1xOFtzsF2WZHQ5tFZ8G9M5AAlJd9e54Yhe3kkygVSfWVyk2IZGae_-ENntIv_pK_f7C4hqw8g";

            var serviceCollection = new ServiceCollection();
            serviceCollection.AddInvestApiClient((_, settings) =>
            {
                settings.AccessToken = token;
            });

            var serviceProvider = serviceCollection.BuildServiceProvider();

            var client = serviceProvider.GetRequiredService<InvestApiClient>();

            var stream = client.MarketDataStream.MarketDataStream();
            //var a = TakeSubscribeForMarketDataByFigi(stream);
            //var b = GetMarketDataByFigi(stream);

            //var stream_2 = client.MarketDataStream.
            var c = TakeSubscribeToTrades(stream);

            var instr = new CandleInstrument[1];





            while (1 == 1)
            {

            }

            Console.WriteLine("Завершено");

        }

        private static async Task TakeSubscribeToTrades(AsyncDuplexStreamingCall<MarketDataRequest, MarketDataResponse> stream)
        {
            var instrs = new TradeInstrument[1];
            instrs[0] = new TradeInstrument { Figi = "BBG004730N88" };
            var req = new SubscribeTradesRequest();
            req.SubscriptionAction = SubscriptionAction.Subscribe;
            req.Instruments.AddRange(instrs);
            Console.WriteLine(req.Instruments.Count);

            // await stream.RequestStream.WriteAsync();
        }


        private static async Task GetMarketDataByFigi(AsyncDuplexStreamingCall<MarketDataRequest, MarketDataResponse> stream)
        {
            // Обрабатываем все приходящие из стрима ответы
            await foreach (var response in stream.ResponseStream.ReadAllAsync())
            {
                Console.WriteLine(JsonSerializer.Serialize(response));
            }
        }

        private static async Task TakeSubscribeForMarketDataByFigi(AsyncDuplexStreamingCall<MarketDataRequest, MarketDataResponse> stream)
        {
            // Отправляем запрос в стрим
            await stream.RequestStream.WriteAsync(new MarketDataRequest
            {
                SubscribeCandlesRequest = new SubscribeCandlesRequest
                {
                    Instruments =
                    {

                        new CandleInstrument
                        {
                            Figi = "BBG004730N88",
                            Interval = SubscriptionInterval.OneMinute
                        }
                    },
                    SubscriptionAction = SubscriptionAction.Subscribe
                }
            });
        }
    }
}