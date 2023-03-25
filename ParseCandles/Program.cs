using FinInvestLibrary.Objects;
using Npgsql;
using System.Globalization;
using System.IO.Compression;

namespace ParseCandles
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            string parrentPath = "D:\\Data";
            string pendingPath = parrentPath + "\\Pending";
            string inWorkPath = parrentPath + "\\InWork";
            string donePath = parrentPath + "\\Done";
            string errorPath = parrentPath + "\\Error";
            string zipPath = parrentPath + "\\Zip";
            string connectionString = "Host=localhost;Username=postgres;Password=#6TY0N0d;Database=FinBase";

            print("Запуск ParseCandles", false);


            //Создаем директории для работы процесса
            print("Создаем директории для работы процесса");
            CreatePaths(pendingPath);
            CreatePaths(inWorkPath);
            CreatePaths(donePath);
            CreatePaths(errorPath);
            CreatePaths(zipPath);

            //Получаем список файлов
            print("Получаем список архивов с историческими свечами");
            var files = GetCandleZipFileList(parrentPath);
            print("Найдено " + files.Count + " архивов");

            //Если есть ZIP файлы - проводим разархивирование
            if (files.Count > 0)
            {
                int totalFiles = files.Count;
                int totalErrors = 0;
                int totalUnziped = 0;

                //Проверяем файлы на существование директорий с аналогичным имененем
                print("Проверяем наличие разархивированного архива");
                foreach (var file in files)
                {
                    string directoryName = file.FileNameWithoutPath.Split('.')[0];
                    bool isDirectotyExist = Directory.Exists(pendingPath + "\\" + directoryName);
                    if (isDirectotyExist)
                    {
                        file.UnzipedDirectoryPath = pendingPath + "\\" + directoryName;
                        file.existUnzipedFilePath = isDirectotyExist;
                        print("Найдена директория " + directoryName + ". Реальный путь " + file.UnzipedDirectoryPath);
                    }
                }


                //Разархивируем файлы, которые не разархивировались раньше
                print("Разархивируем архивы, проверяя на существоваении в папке Pending");
                foreach (var file in files)
                {
                    bool unzipDirectoryName = unZipDirectory(file, pendingPath);
                    if (unzipDirectoryName)
                    {

                        File.Move(file.FileNameWithPath, zipPath + "\\" + file.FileNameWithoutPath);
                        print("Архив" + file.FileNameWithPath + " перемещен в дирректорию " + zipPath);
                        totalUnziped++;
                    }
                    else
                    {

                        File.Move(file.FileNameWithPath, errorPath + "\\" + file.FileNameWithoutPath);
                        print("Архив " + file.FileNameWithPath + " перемещен в " + errorPath, true);
                        totalErrors++;
                    }
                }

                print("Всего обработано архивов: " + totalFiles);
                print("\tИз них разархивировано: " + totalUnziped);
                print("\tНе удалось разархивировать: " + totalErrors);
            }
            else
            {
                print("Отсутсвуют необработанные архивы");
            }

            //Формируем список файлов для записи в БД
            print("Проверяю наличие необработанных исторических свечных файлов ");
            List<HistoryCandleDirectory> historyCandleDirectoryList = GetCandlesDirectoryList(pendingPath);
            print("Найдено " + historyCandleDirectoryList.Count.ToString() + " директорий c необработанными файлами.");

            //Начинаем миграцию данных в БД
            print("Начинаем миграцию данных в БД");
            foreach (var directory in historyCandleDirectoryList)
            {

                print("Обрабатываю директорию: " + directory.path);
                foreach (var file in directory.files)
                {
                    DateTime dateTime = DateTime.Now;
                    print("\tОбрабатываю файл: " + file);
                    StreamReader reader = new StreamReader(file);
                    string[] lines = File.ReadAllLines(file);
                    int counter = 0;

                    foreach (var line in lines)
                    {
                        Candle candle = new Candle();
                        candle.insertdate = dateTime;
                        candle.figi = new DirectoryInfo(directory.path).Name.Split('_')[0];
                        candle.source_filename = file;

                        var returnCandle = convertLine2Candle(line, candle);

                        InsertCandleInDB(candle, connectionString);
                        counter++;
                    }
                    reader.Close();

                    print("Файл " + file + " обработан.");
                    print("Вставлено " + counter + " строк.");
                    //Console.WriteLine(file);
                    //Console.WriteLine(donePath + "\\" + new DirectoryInfo(directory.path).Name.Split('_')[0] + "\\" + Path.GetFileName(file));
                    //File.Move(file, donePath + "\\" + new DirectoryInfo(directory.path).Name.Split('_')[0] + "\\" + Path.GetFileName(file));
                    //print("Файл " + file + " перенесен в " + donePath);
                }

            }




        }

        private static void InsertCandleInDB(Candle candle, string connString)
        {
            Boolean allOK = true;

            using var connection = new NpgsqlConnection(connString);
            try
            {
                connection.Open();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
                allOK = false;
            }

            if (allOK)
            {
                var dbRequest = "INSERT INTO cold_history_candles (figi, candle_start_dt, open_price, close_price, max_price, min_price, volume,source_filename, insertdate, guidfromfile, source) values (@figi, @candle_start_dt, @open_price, @close_price, @max_price, @min_price, @volume,@source_filename, @insertdate, @guidfromfile, @source)";
                try
                {
                    using var command = new NpgsqlCommand(dbRequest, connection);
                    command.Parameters.AddWithValue("figi", candle.figi);
                    command.Parameters.AddWithValue("candle_start_dt", candle.candle_start_dt);
                    command.Parameters.AddWithValue("open_price", candle.open_price);
                    command.Parameters.AddWithValue("close_price", candle.close_price);
                    command.Parameters.AddWithValue("max_price", candle.max_price);
                    command.Parameters.AddWithValue("min_price", candle.min_price);
                    command.Parameters.AddWithValue("volume", candle.volume);
                    command.Parameters.AddWithValue("source_filename", candle.source_filename);
                    command.Parameters.AddWithValue("guidfromfile", candle.guid);
                    command.Parameters.AddWithValue("insertdate", candle.insertdate);
                    command.Parameters.AddWithValue("source", "download_md.sh");
                    command.Prepare();
                    command.ExecuteNonQuery();

                }
                catch (Exception ex)
                {
                    print(ex.ToString());
                }
            }
        }

        private static Candle convertLine2Candle(string line, Candle candle)
        {
            Candle returnCandle = null;

            var data4Candle = line.Split(';');
            candle.guid = data4Candle[0].ToString();
            candle.candle_start_dt = Convert.ToDateTime(data4Candle[1]);
            candle.open_price = float.Parse(data4Candle[2], CultureInfo.InvariantCulture.NumberFormat);
            candle.close_price = float.Parse(data4Candle[3], CultureInfo.InvariantCulture.NumberFormat);
            candle.max_price = float.Parse(data4Candle[4], CultureInfo.InvariantCulture.NumberFormat);
            candle.min_price = float.Parse(data4Candle[5], CultureInfo.InvariantCulture.NumberFormat);
            candle.volume = Convert.ToInt32(data4Candle[6]);
            returnCandle = candle;
            candle = null;
            return returnCandle;
        }

        private static List<HistoryCandleDirectory> GetCandlesDirectoryList(string pendingPath)
        {
            List<HistoryCandleDirectory> returnList = new List<HistoryCandleDirectory>();

            var directorys = Directory.GetDirectories(pendingPath);
            foreach (var directory in directorys)
            {
                try
                {
                    var candleDirectory = new HistoryCandleDirectory();
                    candleDirectory.path = directory;
                    candleDirectory.files = Directory.GetFiles(directory);
                    returnList.Add(candleDirectory);
                }
                catch (Exception ex)
                {
                    print("GetCandlesDirectoryList", true);
                    print(ex.Message, true);
                }
            }
            return returnList;
        }

        private static bool unZipDirectory(HistoryCandleZipDirectory file, string targetPath)
        {
            try
            {
                if (file.existUnzipedFilePath == false)
                {
                    print("Работаю над файлом " + file.FileNameWithoutPath);
                    ZipFile.ExtractToDirectory(file.FileNameWithPath, targetPath + "\\" + file.FileNameWithoutPath.Split('.')[0]);
                    file.existUnzipedFilePath = true;
                    file.UnzipedDirectoryPath = targetPath + "\\" + file.FileNameWithoutPath.Split('.')[0];
                }
                else
                {
                    print("Файл " + file.FileNameWithoutPath + " не требует разархивирования.");
                    return file.existUnzipedFilePath;
                }
            }
            catch (Exception)
            {
                print("В процессе разархивирования файла " + file.FileNameWithPath + " возникла ощибка.");

                return file.existUnzipedFilePath;
            }
            return file.existUnzipedFilePath;
        }

        private static void CreatePaths(string directoryName)
        {
            bool isDirectoryExist = Directory.Exists(directoryName);
            if (isDirectoryExist is false)
            {
                Directory.CreateDirectory(directoryName);
                print("Создана директория " + directoryName + ".");

            }
            else
            {
                print("Директория " + directoryName + " уже сущесвует.");
            }

        }

        private static List<HistoryCandleZipDirectory> GetCandleZipFileList(string directory)
        {
            List<HistoryCandleZipDirectory> historyCandleFiles = new List<HistoryCandleZipDirectory>();
            var files = Directory.GetFiles(directory, "*.zip");

            foreach (var file in files)
            {
                HistoryCandleZipDirectory historyCandleFile = new HistoryCandleZipDirectory();
                historyCandleFile.FileNameWithPath = file;
                historyCandleFile.FileNameWithoutPath = Path.GetFileName(file);
                historyCandleFiles.Add(historyCandleFile);
            }
            return historyCandleFiles;
        }

        private static void print(string message, bool isError)
        {
            if (isError)
                Console.WriteLine("{0} ERROR: {1}", DateTime.Now, message);
            else
                Console.WriteLine("{0} INFO: {1}", DateTime.Now, message);
        }
        private static void print(string message)
        {
            Console.WriteLine("{0} INFO: {1}", DateTime.Now, message);
        }
    }
}