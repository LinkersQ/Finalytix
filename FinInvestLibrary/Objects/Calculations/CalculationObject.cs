namespace FinInvestLibrary.Objects.Calculations
{
    public class CalculationObject
    {
        private int _id;
        private int _candle_id;
        private string _figi;
        private string _candle_scale;
        private string _calc_type;
        private int _duration;
        private float _value;
        private DateTime _insertdate;
        private DateTime _updatedate;

        public int id { get { return _id; } set { _id = value; } }
        public int candle_id { get { return _candle_id; } set { _candle_id = value; } }
        public string figi { get { return _figi; } set { _figi = value; } }
        public string candle_scale { get { return _candle_scale; } set { _candle_scale = value; } }
        public string calc_type { get { return _calc_type; } set { _calc_type = value; } }
        public int duration { get { return _duration; } set { _duration = value; } }
        public float value { get { return _value; } set { _value = value; } }
        public DateTime insertdate { get { return _insertdate; } set { _insertdate = value; } }
        public DateTime updatedate { get { return _updatedate; } set { _updatedate = value; } }


        //13.06.2023
        private int _next_candle_id;
        public int next_candle_id { get { return _next_candle_id; } set { _next_candle_id = value; } }

    }
}
