namespace FinInvestLibrary.Objects
{
    public class HistoryCandleDirectory
    {
        private string? _path;
        private string[]? _files;

        public string path { get { return _path; } set { _path = value; } }
        public string[] files
        {
            get { return _files; }
            set { _files = value; }
        }
    }
}
