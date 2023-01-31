using log4net;
using FinInvestLibrary.Functions.LocalOperations;


namespace CandleWorker
{

    internal class Program
    {
        public static readonly ILog log = LogManager.GetLogger(typeof(Program));
        static int Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();
            log.Info("---Start---");
            log.Info("Зачитываю конфигурацию");
            string file2ExecuteName = string.Empty;
            try
            {
                 file2ExecuteName = args[0];
            }
            catch (Exception ex) 
            {
                log.Error("Не определен файл с командой. Для определения команды добавь к программе аргумент с путем к файлу с командой");
                return 1;
            }
            string command = string.Empty;
            string connectionString = String.Empty;

            GetAppConfiguration(ref connectionString, ref file2ExecuteName, ref command);
            log.Info("Конфигурирование завершено");

            
            PgExecuter pgExecuter = new PgExecuter(connectionString, log);
            
            bool result = pgExecuter.ExecuteNonQuery(command);
            if (result) 
            {
                log.Info("Команда успешно выполнена");
            }
            else { log.Error("Не удалось выполнить команду"); return 1; }

            return 0;

        }
        /// <summary>
        /// Конфигурируем приложение
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="token"></param>
        private static void GetAppConfiguration(ref string connectionString, ref string file2ExecuteName, ref string command )
        {
            try
            {
                //string token = "t.hrRraHICLaGVw1xOFtzsF2WZHQ5tFZ8G9M5AAlJd9e54Yhe3kkygVSfWVyk2IZGae_-ENntIv_pK_f7C4hqw8g";
                //string connectionString = "Host=localhost;Username=postgres;Password=#6TY0N0d;Database=FinBase";
                DateTime currentDateTime = DateTime.UtcNow; //Tinkoff API работает всегда в UTC - придерживаемся тоже UTC;
                string appPath = Environment.CurrentDirectory;
                string connectionStringPath = appPath + "\\connectionString.txt";
                connectionString = File.ReadAllText(connectionStringPath);
                string tokenPath = appPath + "\\token.txt";
                command = File.ReadAllText(file2ExecuteName);
                log.Info("Команда: " + connectionString);
            }
            catch (Exception ex)
            {
                log.Error("Возникла ошибка в процессе конфишурирования настроек");
                log.Error(ex.ToString());
            }
        }
    }
}