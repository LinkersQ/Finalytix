namespace FinInvestLibrary.Objects
{
    public class HistoryCandleZipDirectory
    {
        private string _FileNameWithPath;
        private string _FileNameWithoutPath;
        private int _FileSize;
        private string _unzipedFilePath;
        private bool _existUnzipedFilePath;

        public string FileNameWithPath { get { return _FileNameWithPath; } set { _FileNameWithPath = value; } }
        public string FileNameWithoutPath { get { return _FileNameWithoutPath; } set { _FileNameWithoutPath = value; } }
        public int FileSize
        {
            get { return _FileSize; }
            set
            {
                _FileSize = value;
            }
        }
        public string UnzipedDirectoryPath { get { return _unzipedFilePath; } set { _unzipedFilePath = value; } }
        public bool existUnzipedFilePath { get { return _existUnzipedFilePath; } set { _existUnzipedFilePath = value; } }
    }
}
