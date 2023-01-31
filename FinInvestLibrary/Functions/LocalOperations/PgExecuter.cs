using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;
using System.Net;

namespace FinInvestLibrary.Functions.LocalOperations
{
    public class PgExecuter
    {
        string connectionString = string.Empty;
        NpgsqlConnection connection = null;
        ILog log = null;
        /// <summary>
        /// Инцициализация класса
        /// </summary>
        /// <param name="connString">строка подключения к бд</param>
        /// <param name="log">объект логгера для низкоуровнего логирования</param>
        public PgExecuter(string connString, ILog inputlog) 
        {
            connectionString = connString;
            connection = new NpgsqlConnection(connString);
            log = inputlog;
        }

        public bool ExecuteNonQuery(string SQLCommand)
        { 
            bool result = false;
            log.Info("Получена команда на запуск скрипта:");
            log.Info(SQLCommand);
            log.Info("Подключаюсь к БД...");
            try
            {
                connection.Open();
                log.Info("Подключение устрановлено");
            }
            catch(Exception ex) 
            {
                log.Error("Не удалось установить подключение");
                log.Error(ex.ToString());
                return result;
            }
            DateTime executeStartDT = DateTime.Now.ToUniversalTime();
            try
            { 
                using var command = new NpgsqlCommand(SQLCommand, connection);
                command.ExecuteNonQuery();
                result = true;
            }
            catch (Exception ex) 
            {
                log.Error("Не удалось выполнить команду");
                log.Error(ex.ToString());
            }
            DateTime executeFinishDT = DateTime.Now.ToUniversalTime();
            log.Info("Время выполнения: " + (executeFinishDT - executeStartDT).TotalSeconds + " секунд.");

            return result;
        }
    }
}
