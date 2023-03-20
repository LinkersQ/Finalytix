using System;
using System.Threading;
using System.Threading.Tasks;
using Telegram.Bot;
using Telegram.Bot.Extensions.Polling;
using Telegram.Bot.Types;
using Telegram.Bot.Exceptions;
using System.IO;
using log4net;
using System.Collections.Generic;
using FinGrowPointPublisherAPP.DataFunctions;
using FinGrowPointPublisherAPP.Objects;
using System.Security.Cryptography.X509Certificates;
using Newtonsoft.Json;

namespace FinGrowPointPublisherAPP
{
    class Program
    {

        public static readonly ILog log = LogManager.GetLogger(typeof(Program));
        static string TG_BOT_TOKEN = string.Empty;
        static long TG_CHANNEL_ID = -1;
        static string OPEN_TRADE_TEMPLATE = string.Empty;
        static string CLOSE_TRADE_TEMPLATE = string.Empty;
        static string CONNECTION_STRING = string.Empty;
        static string TEMPLATES_PATH = string.Empty;


        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();
            log.Info(@"/---------Start---------\");

            bool configStatus = GetAppConfiguration(ref TG_BOT_TOKEN, ref TG_CHANNEL_ID);
            if (configStatus)
            {
                ITelegramBotClient bot = new TelegramBotClient(TG_BOT_TOKEN);
                GetAppConfiguration(ref CONNECTION_STRING);
                log.Info("Получаю список новых сообщений для отправки:");
                var commList = GetNewCommunications();
                log.Info("Найдено " + commList.Count + " новых сообщений для отправки");

                log.Info("Готовлючь к отправке сообщений");
                var messages = GetMessagesFromJSON(commList);
                var templateList = GetTemplateListWithTags(messages);

                foreach (var mes in templateList)
                {
                    SendTradeMessage(bot, mes.final_message, long.Parse(mes.channel_id));
                    Thread.Sleep(2000);
                }






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
            try
            {
                Message mes = new Message();
                mes.Chat = new Chat { Id = channel_id, };
                var res = await botClient.SendTextMessageAsync(mes.Chat, template2Send);
                Console.WriteLine(res.MessageId + " - " + res.Text);
            }
            catch (Exception ex) 
            {
                log.Error($"Error {ex.Message}");
            }
        }


        private static List<MessageContent> GetTemplateListWithTags(List<MessageContent> messages)
        {
            List<MessageContent> result = new List<MessageContent>();   
            foreach (var message in messages)
            {
                string templateTXT = System.IO.File.ReadAllText(TEMPLATES_PATH + "\\" + message.message_template_name);

                templateTXT = templateTXT.Replace("%TIKER%", message.ticker);
                templateTXT = templateTXT.Replace("%TRADE_TYPE%", message.trade_type);
                templateTXT = templateTXT.Replace("%OPEN_PRICE%", message.open_price);
                templateTXT = templateTXT.Replace("%STOP_LOSS_PRICE%", message.stop_loss_price);
                templateTXT = templateTXT.Replace("%PROFIT_1_PRICE%", message.target_price_1);
                templateTXT = templateTXT.Replace("%PROFIT_2_PRICE%", message.target_price_2);
                templateTXT = templateTXT.Replace("%TRADE_DURATION%", message.trade_dur_forecast);
                templateTXT = templateTXT.Replace("%TRADE_ID%", message.trade_id);
                message.final_message = templateTXT;
                result.Add(message);

            }
            return result;
        }

        private static List<MessageContent> GetMessagesFromJSON(List<CommunicationObject> commList)
        {
            List<MessageContent> messages = new List<MessageContent>();
            foreach (var comm in commList)
            {
                var obj = JsonConvert.DeserializeObject<MessageContent>(comm.message_content);
                messages.Add(obj);
                
            }
            return messages;

        }

        static List<CommunicationObject> GetNewCommunications()
        {
            var communicationsStrings = new PgExecuter(CONNECTION_STRING, log).ExecuteReader("SELECT id, external_id, create_dt, message_content, message_media, communication_dt, communication_status, inform_messages, communication_id_from_channel FROM public.communications ORDER BY create_dt");

            List<CommunicationObject> communicationObjectList = new List<CommunicationObject>();
            foreach (var str in communicationsStrings)
            {
                var partsOfRow = str.Split(';');

                CommunicationObject commObject = new CommunicationObject();
                commObject.id = partsOfRow[0].ToString();
                commObject.external_id = partsOfRow[1].ToString();
                commObject.create_dt = Convert.ToDateTime(partsOfRow[2]);
                commObject.message_content = partsOfRow[3].ToString();
                commObject.message_media = partsOfRow[4];
                commObject.communication_dt = partsOfRow[5].Length > 5 ? Convert.ToDateTime(partsOfRow[7]) : Convert.ToDateTime("2000-01-01 00:00:00");
                commObject.communication_status = partsOfRow[6];
                commObject.inform_messages = partsOfRow[7];
                commObject.communication_id_from_channel = partsOfRow[8];


                communicationObjectList.Add(commObject);
            }
            
            return communicationObjectList;
        }






        /// <summary>
        /// Конфигурируем приложение
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="token"></param>
        private static bool GetAppConfiguration(ref string connectionString)
        {
            bool result = false;
            try
            {
                //string token = "t.hrRraHICLaGVw1xOFtzsF2WZHQ5tFZ8G9M5AAlJd9e54Yhe3kkygVSfWVyk2IZGae_-ENntIv_pK_f7C4hqw8g";
                //string connectionString = "Host=localhost;Username=postgres;Password=#6TY0N0d;Database=FinBase";
                DateTime currentDateTime = DateTime.UtcNow; //Tinkoff API работает всегда в UTC - придерживаемся тоже UTC;
                string appPath = Environment.CurrentDirectory;
                string connectionStringPath = appPath + "\\connectionString.txt";
                connectionString = System.IO.File.ReadAllText(connectionStringPath);

                TEMPLATES_PATH = "D:\\MessagesTemplates";


                result = true;
            }
            catch (Exception ex)
            {
                log.Error("Возникла ошибка в процессе конфигурирования настроек");
                log.Error(ex.ToString());
            }
            return result;
        }








        public static async Task HandleUpdateAsync(ITelegramBotClient botClient, Update update, CancellationToken cancellationToken)
        {
            // Некоторые действия
            Console.WriteLine(Newtonsoft.Json.JsonConvert.SerializeObject(update));
            if (update.Type == Telegram.Bot.Types.Enums.UpdateType.Message)
            {
                var message = update.Message;
                try
                {
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
                }
                catch
                (Exception ex)
                {
                    Message mes = new Message();
                    //mes.ReplyToMessage = new Message { MessageId = 17 };
                    mes.Chat = new Chat { Id = TG_CHANNEL_ID };
                    var res = await botClient.SendTextMessageAsync(mes.Chat, ex.ToString());
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


        

        private static bool GetAppConfiguration(ref string TG_BOT_TOKEN, ref long TG_CHANNEL_ID)
        {
            bool result = false;
            try
            {
                //string token = "t.hrRraHICLaGVw1xOFtzsF2WZHQ5tFZ8G9M5AAlJd9e54Yhe3kkygVSfWVyk2IZGae_-ENntIv_pK_f7C4hqw8g";
                //string connectionString = "Host=localhost;Username=postgres;Password=#6TY0N0d;Database=FinBase";
                
                DateTime currentDateTime = DateTime.UtcNow; //Tinkoff API работает всегда в UTC - придерживаемся тоже UTC;
                string appPath = Environment.CurrentDirectory;
                //string OPEN_TRADE_TEMPLATE_Path = appPath + "\\Configurations\\Templates\\OPEN_TRADE_TEMPLATE.txt";
                //string CLOSE_TRADE_TEMPLATE_Path = appPath + "\\Configurations\\Templates\\CLOSE_TRADE_TEMPLATE.txt";
                string TG_BOT_TOKEN_Path = appPath + "\\Configurations\\TG_TOKEN.txt";
                string TG_CHANNEL_ID_Path = appPath + "\\Configurations\\MT_CHANNEL_ID.txt";

                //OPEN_TRADE_TEMPLATE = System.IO.File.ReadAllText(OPEN_TRADE_TEMPLATE_Path);
                //CLOSE_TRADE_TEMPLATE = System.IO.File.ReadAllText(CLOSE_TRADE_TEMPLATE_Path);
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
