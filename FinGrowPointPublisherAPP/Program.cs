using System;
using System.Threading;
using System.Threading.Tasks;
using Telegram.Bot;
using Telegram.Bot.Extensions.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Exceptions;
using System.IO;
using log4net;

namespace FinGrowPointPublisherAPP
{
    class Program
    {

        public static readonly ILog log = LogManager.GetLogger(typeof(Program));
        static string TG_BOT_TOKEN = string.Empty;
        static long TG_CHANNEL_ID = -1;
        static string OPEN_TRADE_TEMPLATE = string.Empty;
        static string CLOSE_TRADE_TEMPLATE = string.Empty;


        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();
            log.Info(@"/---------Start---------\");

            bool configStatus = GetAppConfiguration(ref OPEN_TRADE_TEMPLATE, ref CLOSE_TRADE_TEMPLATE, ref TG_BOT_TOKEN, ref TG_CHANNEL_ID);
            if (configStatus)
            {
                ITelegramBotClient bot = new TelegramBotClient(TG_BOT_TOKEN);

                log.Info("Запущен бот " + bot.GetMeAsync().Result.FirstName);
              


                var cts = new CancellationTokenSource();
                var cancellationToken = cts.Token;
                var receiverOptions = new ReceiverOptions
                {
                    AllowedUpdates = { }, // receive all update types
                };
                bot.StartReceiving(
                    HandleUpdateAsync,
                    HandleErrorAsync,
                    receiverOptions,
                    cancellationToken
                );
                Console.ReadLine();
            }
            else
            {
                log.Error("В процессе конфигурирования приложения возникла ошибка. Приложение завершено.");
            }
        }

        public static async void SendTradeMessage(ITelegramBotClient botClient, string template2Send, long channel_id)
        {
            Message mes = new Message();
            mes.Chat = new Chat { Id = channel_id, };
            var res = await botClient.SendTextMessageAsync(mes.Chat, template2Send);
            Console.WriteLine(res.MessageId + " - " + res.Text);
        }

        public static async Task HandleUpdateAsync(ITelegramBotClient botClient, Update update, CancellationToken cancellationToken)
        {
            // Некоторые действия
            Console.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(update));
            if (update.Type == Telegram.Bot.Types.Enums.UpdateType.Message)
            {
                var message = update.Message;
                if (message.Text.ToLower() == "open")
                {
                    Message mes = new Message();
                    //mes.ReplyToMessage = new Message { MessageId = 17 };
                    mes.Chat = new Chat { Id = -1001669467340, };
                    var res = await botClient.SendTextMessageAsync(mes.Chat, OPEN_TRADE_TEMPLATE);
                    Console.WriteLine(res.MessageId + " - " + res.Text);
                    return;
                }
                else if (message.Text.ToLower() == "close")
                {
                    Message mes = new Message();
                    //mes.ReplyToMessage = new Message { MessageId = 17 };
                    mes.Chat = new Chat { Id = TG_CHANNEL_ID };
                    var res = await botClient.SendTextMessageAsync(mes.Chat, CLOSE_TRADE_TEMPLATE);
                    Console.WriteLine(res.MessageId + " - " + res.Text);
                    return;
                }

                //await botClient.SendTextMessageAsync(message.Chat, "Привет-привет!!");
            }
        }

        public static async Task HandleErrorAsync(ITelegramBotClient botClient, Exception exception, CancellationToken cancellationToken)
        {
            // Некоторые действия
            Console.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(exception));
        }


        

        private static bool GetAppConfiguration(ref string OPEN_TRADE_TEMPLATE, ref string CLOSE_TRADE_TEMPLATE, ref string TG_BOT_TOKEN, ref long TG_CHANNEL_ID)
        {
            bool result = false;
            try
            {
                //string token = "t.hrRraHICLaGVw1xOFtzsF2WZHQ5tFZ8G9M5AAlJd9e54Yhe3kkygVSfWVyk2IZGae_-ENntIv_pK_f7C4hqw8g";
                //string connectionString = "Host=localhost;Username=postgres;Password=#6TY0N0d;Database=FinBase";
                
                DateTime currentDateTime = DateTime.UtcNow; //Tinkoff API работает всегда в UTC - придерживаемся тоже UTC;
                string appPath = Environment.CurrentDirectory;
                string OPEN_TRADE_TEMPLATE_Path = appPath + "\\Configurations\\Templates\\OPEN_TRADE_TEMPLATE.txt";
                string CLOSE_TRADE_TEMPLATE_Path = appPath + "\\Configurations\\Templates\\CLOSE_TRADE_TEMPLATE.txt";
                string TG_BOT_TOKEN_Path = appPath + "\\Configurations\\TG_TOKEN.txt";
                string TG_CHANNEL_ID_Path = appPath + "\\Configurations\\MT_CHANNEL_ID.txt";

                OPEN_TRADE_TEMPLATE = System.IO.File.ReadAllText(OPEN_TRADE_TEMPLATE_Path);
                CLOSE_TRADE_TEMPLATE = System.IO.File.ReadAllText(CLOSE_TRADE_TEMPLATE_Path);
                TG_BOT_TOKEN = System.IO.File.ReadAllText(TG_BOT_TOKEN_Path);
                TG_CHANNEL_ID = long.Parse(System.IO.File.ReadAllText(TG_CHANNEL_ID_Path).ToString());

                result = true;
            }
            catch (Exception ex)
            {
                log.Error("Возникла ошибка в процессе конфигурирования настроек");
                log.Error(ex.ToString());
            }
            return result;
        }
    }
}
