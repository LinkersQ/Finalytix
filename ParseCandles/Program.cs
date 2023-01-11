using System.Configuration;
using System.IO.Compression;
using System.Reflection;
using System.Runtime.InteropServices;
using FinInvestLibrary.Objects;

namespace ParseCandles
{
    internal class Program
    {


        static void Main(string[] args)
        {
            string parrentPath = "D:\\Data";
            string pendingPath = parrentPath + "\\Pending";
            string inWorkPath = parrentPath + "\\InWork";
            string donePath = parrentPath + "\\Done";
            string errorPath = parrentPath + "\\Error";
            string zipPath = parrentPath + "\\Zip";

            print("Запуск ParseCandles", false);
            

            //Создаем директории для работы процесса
            print("Создаем директории для работы процесса");
            CreatePaths(pendingPath);
            CreatePaths(inWorkPath);
            CreatePaths(donePath);
            CreatePaths(errorPath);
            CreatePaths(zipPath);

            //Получаем список файлов
            print("Получаем список файлов");
            var files = GetCandleFileList(parrentPath);
            print("Найдено " + files.Count + " файлов");

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
            print("Разархивируем файлы, проверяя на существоваении в папке Pending");
            foreach (var file in files)
            {
                bool unzipDirectoryName = unZipFiles(file, pendingPath);
                if (unzipDirectoryName)
                {
                    
                    File.Move(file.FileNameWithPath, zipPath + "\\" + file.FileNameWithoutPath);
                    print(file.FileNameWithPath + " успешно разархивирован и перемещен в дирректорию " + zipPath);
                }
                else
                {
                    
                    File.Move(file.FileNameWithPath, errorPath + "\\" + file.FileNameWithoutPath);
                    print("Не удалось разархивировать файл " + file.FileNameWithPath + ". Перемещен в " + errorPath);
                }
            }
        }

        private static bool unZipFiles(HistoryCandleFile file, string targetPath)
        {
            try
            {
                print("Работаю над файлом " + file.FileNameWithoutPath);
                ZipFile.ExtractToDirectory(file.FileNameWithPath, targetPath + "\\" + file.FileNameWithoutPath.Split('.')[0]);
                file.existUnzipedFilePath = true;
            }
            catch (Exception ex) 
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

        private static List<HistoryCandleFile> GetCandleFileList(string directory)
        {
            List<HistoryCandleFile> historyCandleFiles = new List<HistoryCandleFile>();
            var files = Directory.GetFiles(directory, "*.zip");

            foreach (var file in files)
            {
                HistoryCandleFile historyCandleFile = new HistoryCandleFile();
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