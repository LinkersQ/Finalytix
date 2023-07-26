using FinInvestLibrary.Functions.LocalOperations;
using log4net;

namespace QueryExecuter
{
    internal class Program
    {
        public static readonly ILog log = LogManager.GetLogger(typeof(Program));
        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();

            string query, queryFilePath, connectionString, outputPath, outputFileName;

            log.Info("Start...");

            try
            {
#if DEBUG
                string appPath = Environment.CurrentDirectory;
                string connectionStringPath = appPath + "\\connectionString.txt";
                queryFilePath = "D:\\Dev\\Querys\\TestQuery.sql";
                outputPath = "D:\\Dev\\Querys";
                outputFileName = "ResultQuery.txt";
#else
                string appPath = Environment.CurrentDirectory;
                string connectionStringPath = appPath + "\\connectionString.txt";
                connectionString = File.ReadAllText(connectionStringPath);

                queryFilePath = args[0];
                query = File.ReadAllText(queryFilePath);

                outputPath = args[1];
                outputFileName = args[2];
#endif
                //[queryFilePath] [outputFilePath] [outputFileName]
                connectionString = File.ReadAllText(connectionStringPath);
                query = File.ReadAllText(queryFilePath);

                log.Info("Текущая конфигурация:");
                log.Info("\tconnectionString: " + connectionString);
                log.Info("\tqueryFilePath: " + queryFilePath);
                log.Info("\toutputPath: " + outputPath);
                log.Info("\toutputFileName: " + outputFileName);
                log.Info("\tquery: " + query);

                log.Info("Выполняю запрос к БД...");
                var result = new PgExecuter(connectionString, log).ExecuteScalarQuery(query);
                if (result.Length == 0)
                {
                    result = "-1";
                    log.Info("Результат запроса вернул NULL");
                    log.Info("В выходной файл будет записано значение " + result);
                }
                else
                {
                    log.Info("Результат запроса вернул " + result);
                }
                try
                {
                    log.Info("Записываю результат в выходной файл");
                    File.WriteAllText(outputPath + "\\" + outputFileName, result);
                    log.Info("Запись успешно завершена");
                }
                catch (Exception ex)
                {
                    log.Error("Не удалось записать результат в выходной файл");
                    log.Error(ex.ToString());
                }
                log.Info("Программа завершена.");
            }
            catch (Exception ex)
            {
                log.Error("Не удалось прочитать параметры конфигурирования приложения.");
                log.Error(ex.ToString());
            }
        }
    }
}