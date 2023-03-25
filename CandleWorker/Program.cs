using FinInvestLibrary.Functions.LocalOperations;
using log4net;


namespace CandleWorker
{

    internal class Program
    {
        public static readonly ILog log = LogManager.GetLogger(typeof(Program));

        private static int Main(string[] args)
        {
            int exitCode = 9999;

            log4net.Config.XmlConfigurator.Configure();
            log.Info(@"/---------Start---------\");
            log.Info("Зачитываю конфигурацию");

            string file2ExecuteName = string.Empty;
            try
            {
                file2ExecuteName = args[0];
                string command = string.Empty;
                string connectionString = String.Empty;

                bool configResult = GetAppConfiguration(ref connectionString, ref file2ExecuteName, ref command);

                if (configResult)
                {
                    log.Info("Конфигурирование успешно завершено");
                    PgExecuter pgExecuter = new PgExecuter(connectionString, log);

                    bool commExecuteResult = pgExecuter.ExecuteNonQuery(command);
                    if (commExecuteResult)
                    {

                        exitCode = 0;
                    }
                    else
                    {
                        exitCode = 1;
                    }
                }
                else
                {
                    exitCode = 1;
                }
            }
            catch (Exception)
            {
                log.Error("Не определен файл с командой. Для определения команды добавь к программе аргумент с путем к файлу с командой");
                exitCode = 1;
            }
            log.Info(@"\---------END---------/");
            return exitCode;

        }
        /// <summary>
        /// Конфигурируем приложение
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="token"></param>
        private static bool GetAppConfiguration(ref string connectionString, ref string file2ExecuteName, ref string command)
        {
            bool result = false;
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
                log.Info("Строка подключения: " + connectionString);
                log.Info("Использую файл с инструкцией: " + file2ExecuteName);
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