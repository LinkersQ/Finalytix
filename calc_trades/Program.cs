
namespace calc_trades
{
    internal class Program
    {
        static void Main(string[] args)
        {
            
        }

        public List<string> ExecuteReader(string SQLCommand)
        {
            List<string> returnListString = new List<string>();
            log.Debug("Подключаюсь к БД...");
            try
            {
                connection.Open();
                log.Debug("Подключение устрановлено");
            }
            catch (Exception ex)
            {
                log.Error("Не удалось установить подключение");
                log.Error(ex.ToString());
                connection.Close();
            }

            DateTime executeStartDT = DateTime.Now.ToUniversalTime();
            try
            {
                log.Debug("Выполняю полученную инструкцию SQL");
                log.Debug(SQLCommand);
                using var command = new NpgsqlCommand(SQLCommand, connection);
                command.CommandTimeout = 600;
                using NpgsqlDataReader reader = command.ExecuteReader();

                while (reader.Read())
                {
                    string row = string.Empty;
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row = row + reader[i].ToString();
                        if (i != reader.FieldCount)
                        {
                            row = row + ";";
                        }
                    }
                    returnListString.Add(row);
                }
                connection.Close();

            }
            catch (Exception ex)
            {
                log.Error("Не удалось выполнить инструкцию");
                log.Error(ex.ToString());
                connection.Close();
                return returnListString;
            }
            DateTime executeFinishDT = DateTime.Now.ToUniversalTime();
            log.Debug("Инструкция успешно выполнена");
            log.Debug("Время выполнения: " + (executeFinishDT - executeStartDT).TotalSeconds + " секунд.");

            return returnListString;


        }
    }
}