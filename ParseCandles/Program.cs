namespace ParseCandles
{
    internal class Program
    {
        
        static void Main(string[] args)
        {
            string parrentPath = "D:\\data";
            string[] files = Directory.GetFiles(parrentPath);
            print(files.Count().ToString());

            
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