namespace FinInvestLibrary.Objects.Trade
{
    public class CandleForSMAStratAnalysis
    {
        private int _candleId;
        private string _figi;
        private DateTime _candleOpenDt;
        private float _fastInterval;
        private float _slowInterval;
        private float _openPrice;
        private float _closePrice;
        private float _minPrice;
        private float _maxPrice;




        public int candleId { get { return _candleId; } set { _candleId = value; } }
        public string figi { get { return _figi; } set { _figi = value; } }
        public DateTime candleOpenDt { get { return _candleOpenDt; } set { _candleOpenDt = value; } }
        public float fastInterval { get { return _fastInterval; } set { _fastInterval = value; } }
        public float slowInterval { get { return _slowInterval; } set { _slowInterval = value; } }
        public float openPrice { get { return _openPrice; } set { _openPrice = value; } }
        public float closePrice { get { return _closePrice; } set { _closePrice = value; } }
        public float minPrice { get { return _minPrice; } set { _minPrice = value; } }
        public float maxPrice { get { return _maxPrice; } set { _maxPrice = value; } }
    }
}
