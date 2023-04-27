using FinGrowPointPublisherAPP.DataFunctions;
using FinGrowPointPublisherAPP.Objects;
using log4net;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Telegram.Bot;
using Telegram.Bot.Types;

namespace FinGrowPointPublisherAPP
{
    internal class Program
    {

        public static readonly ILog log = LogManager.GetLogger(typeof(Program));
        private static string TG_BOT_TOKEN = string.Empty;
        private static long TG_CHANNEL_ID = -1;
        private static string OPEN_TRADE_TEMPLATE = string.Empty;
        private static string CLOSE_TRADE_TEMPLATE = string.Empty;
        private static string CONNECTION_STRING = string.Empty;
        private static string TEMPLATES_PATH = string.Empty;

        private static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();
            log.Info(@"/---------Start---------\");

            bool configStatus = GetAppConfiguration(ref TG_BOT_TOKEN, ref TG_CHANNEL_ID);
            if (configStatus)
            {
                ITelegramBotClient bot = new TelegramBotClient(TG_BOT_TOKEN);
                GetAppConfiguration(ref CONNECTION_STRING);


                while (1 == 1)
                {
                    log.Info("Получаю список новых сообщений для отправки:");
                    var commList = GetNewCommunications();
                    log.Info("Найдено " + commList.Count + " новых сообщений для отправки");

                    log.Info("Готовлючь к отправке сообщений");
                    var messages = GetMessagesFromJSON(commList);
                    var templateList = GetTemplateListWithTags(messages);
                    log.Info("Сформировано " + templateList.Count + " новых шаблонов для отправки");

                    if (templateList.Count > 0)
                    {
                        foreach (var mes in templateList)
                        {
                            Thread.Sleep(5000);
                            var commObj = commList.FirstOrDefault(f => f.external_id.Equals(mes.trade_id));
                            SendTradeMessage(bot, mes.final_message, long.Parse(mes.channel_id), commObj.id);

                        }
                    }
                    else
                    {
                        log.Info("Нет новых сообщений и шаблонов");
                    }
                    Thread.Sleep(5000);
                    bot.CloseAsync();
                }

            }
            else
            {
                log.Error("В процессе конфигурирования приложения возникла ошибка. Приложение завершено.");
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="commId">Идентификатор коммуникационной строки в таблице</param>
        /// <param name="communication_status">статус отправки коммуникации</param>
        /// <param name="inform_mes">произвольная информация</param>
        /// <param name="messId">идентификатор коммуникации, назначенный в канале коммуниации</param>
        private static void UpdateCommunicationRow(string commId, string communication_status, string inform_mes, string messId)
        {
            string sqlCommand = "UPDATE public.communications SET communication_dt = '" + DateTime.Now.ToString() + "', communication_status = '" + communication_status + "', inform_messages = '" + inform_mes + "', communication_id_from_channel = '" + messId + "' where id = '" + commId + "'";

            new PgExecuter(CONNECTION_STRING, log).ExecuteNonQuery(sqlCommand);
        }

        public static async void SendTradeMessage(ITelegramBotClient botClient, string template2Send, long channel_id, string communicationId)
        {

            try
            {
                Message mes = new Message();
                mes.Chat = new Chat { Id = channel_id, };
                UpdateCommunicationRow(communicationId, "DONE", "ALL OK", "");
                var res = await botClient.SendTextMessageAsync(mes.Chat, template2Send);
                UpdateCommunicationRow(communicationId, "DONE", "ALL OK", res.MessageId.ToString());

            }
            catch (Exception ex)
            {
                log.Error($"Error {ex.Message}");
                UpdateCommunicationRow(communicationId, "ERROR", ex.ToString(), "");
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
                templateTXT = templateTXT.Replace("%TRADE_RESULT%", message.stop_loss_perc);
                templateTXT = templateTXT.Replace("%NAME%", message.name);
                templateTXT = templateTXT.Replace("%SHORT_AVAILABLE_FLG%", message.tinkoffAvailableShort);
                templateTXT = templateTXT.Replace("%PROFIT_1_PERC%", message.target_perc_1);
                templateTXT = templateTXT.Replace("%PROFIT_2_PERC%", message.target_perc_2);
                templateTXT = templateTXT.Replace("STOP_LOSS_PERC", message.stop_loss_perc);
                templateTXT = templateTXT.Replace("%STOP_LOSS_PRICE_FOR_PROFIT_2%", message.stop_loss_price_for_profit_2);
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

        private static List<CommunicationObject> GetNewCommunications()
        {
            var communicationsStrings = new PgExecuter(CONNECTION_STRING, log).ExecuteReader("SELECT id, external_id, create_dt, message_content, message_media, communication_dt, communication_status, inform_messages, communication_id_from_channel FROM public.communications WHERE communication_status is null or communication_status = 'ERROR' ORDER BY create_dt");

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
                commObject.communication_dt = partsOfRow[5].Length > 5 ? Convert.ToDateTime(partsOfRow[5]) : Convert.ToDateTime("2000-01-01 00:00:00");
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
