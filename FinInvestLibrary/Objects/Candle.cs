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



        //02.05.2023
        private int _id;
        public int id { get { return _id; } set { _id = value; } }

        private string _scale;
        public string scale { get { return _scale; } set { _scale = value; } }

        //13.06.2023
        private bool _isDelete;
        public bool isDelete { get { return _isDelete; } set { _isDelete = value; } }

        //03.07.2023 
        //добавлены для работы с приложением стратегий StrategyAPP
        private Calculations.CalculationObject _calculation_current_fast;
        private Calculations.CalculationObject _calculation_current_slow;

        public Calculations.CalculationObject calculation_current_fast { get { return _calculation_current_fast; } set { _calculation_current_fast = value; } }
        public Calculations.CalculationObject calculation_current_slow { get { return _calculation_current_slow; } set { _calculation_current_slow = value; } }



        private Calculations.CalculationObject _calculation_prev_fast;
        private Calculations.CalculationObject _calculation_prev_slow;

        public Calculations.CalculationObject calculation_prev_fast { get { return _calculation_prev_fast; } set { _calculation_prev_fast = value; } }
        public Calculations.CalculationObject calculation_prev_slow { get { return _calculation_prev_slow; } set { _calculation_prev_slow = value; } }
    }
}
