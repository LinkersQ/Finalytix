namespace FinInvestLibrary.Objects
{
    public class Candle
    {
        private string? _figi;
        private DateTime? _candle_start_dt;
        private float? _open_price;
        private float? _close_price;
        private float? _max_price;
        private float? _min_price;
        private int? _volume;
        private string? _source_filename;
        private DateTime? _insertdate;
        private string? _guid;
        private bool _isParsingError;

        public string? figi { get { return _figi; } set { _figi = value; } }
        public DateTime? candle_start_dt { get { return _candle_start_dt; } set { _candle_start_dt = value; } }
        public float? open_price { get { return _open_price; } set { _open_price = value; } }
        public float? close_price { get { return _close_price; } set { _close_price = value; } }
        public float? max_price { get { return _max_price; } set { _max_price = value; } }
        public float? min_price { get { return _min_price; } set { _min_price = value; } }
        public int? volume { get { return _volume; } set { _volume = value; } }
        public string? source_filename { get { return _source_filename; } set { _source_filename = value; } }
        public DateTime? insertdate { get { return _insertdate; } set { _insertdate = value; } }
        public string? guid { get { return _guid; } set { _guid = value; } }
        public bool isParsingError { get { return _isParsingError; } set { _isParsingError = value; } }

    }
}
