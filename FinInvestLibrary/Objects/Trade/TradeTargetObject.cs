using System.Globalization;

namespace MAStrategyApp
{
    public class TradeTargetObject
    {
        private string _figi;
        private string _ticker;
        private string _stratname;
        private string _tradeType;
        private float _target_1;
        private float _target_2;
        private float _target_1_duration;
        private float _target_2_duration;
        private float _stop_loss;

        public string figi { get { return _figi; } set { _figi = value; } }
        public string ticker { get { return _ticker; } set { _ticker = value; } }
        public string stratname { get { return _stratname; } set { _stratname = value; } }
        public string tradeType { get { return _tradeType; } set { _tradeType = value; } }
        public float target_1 { get { return _target_1;} set { _target_1 = value; } }
        public float target_2 { get { return _target_2;} set { _target_2 = value; } }
        public float target_1_duration { get { return _target_1_duration; } set { _target_1_duration = value; } }
        public float target_2_duration { get { return _target_2_duration; } set { _target_2_duration = value; } }
        public float stop_loss { get { return _stop_loss; } set { _stop_loss = value; } }
    
    }
}
