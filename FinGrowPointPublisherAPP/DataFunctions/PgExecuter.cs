using log4net;
using Npgsql;
using System;
using System.Collections.Generic;

namespace FinGrowPointPublisherAPP.DataFunctions
{
    public class PgExecuter
    {
        private string connectionString = string.Empty;
        private NpgsqlConnection connection = null;
        private ILog log = null;
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
                return result;
            }
            DateTime executeStartDT = DateTime.Now.ToUniversalTime();
            try
            {
                log.Debug("Выполняю полученную инструкцию SQL");
                log.Debug(SQLCommand);
                using var command = new NpgsqlCommand(SQLCommand, connection);
                command.CommandTimeout = 600;
                command.ExecuteNonQuery();

                result = true;
            }
            catch (Exception ex)
            {
                log.Error("Не удалось выполнить инструкцию");
                log.Error(ex.ToString());
                connection.Close();
                return result;
            }
            DateTime executeFinishDT = DateTime.Now.ToUniversalTime();
            log.Debug("Инструкция успешно выполнена");
            log.Debug("Время выполнения: " + (executeFinishDT - executeStartDT).TotalSeconds + " секунд.");
            connection.Close();
            return result;
        }

        public string ExecuteScalarQuery(string SQLCommand)
        {
            string returnStr = string.Empty;

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
                return returnStr;
            }

            DateTime executeStartDT = DateTime.Now.ToUniversalTime();

            try
            {
                log.Debug("Выполняю полученную инструкцию SQL");
                log.Debug(SQLCommand);
                using var command = new NpgsqlCommand(SQLCommand, connection);
                command.CommandTimeout = 600;
                var res = command.ExecuteScalar();
                returnStr = res.ToString();
                connection.Close();

            }
            catch (Exception ex)
            {
                log.Error("Не удалось выполнить инструкцию");
                log.Error(ex.ToString());
                log.Error(SQLCommand);
            }
            DateTime executeFinishDT = DateTime.Now.ToUniversalTime();
            log.Debug("Инструкция успешно выполнена");
            log.Debug("Время выполнения: " + (executeFinishDT - executeStartDT).TotalSeconds + " секунд.");
            return returnStr;

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
                return returnListString;
            }
            DateTime executeFinishDT = DateTime.Now.ToUniversalTime();
            log.Debug("Инструкция успешно выполнена");
            log.Debug("Время выполнения: " + (executeFinishDT - executeStartDT).TotalSeconds + " секунд.");

            return returnListString;


        }

    }
}
