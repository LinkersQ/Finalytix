namespace FinInvestLibrary.Objects.StrategyObjects
{
    public class BorderPoints
    {
        private string _figi;
        private string _strat_name;
        private string _strat_type;
        private string _strat_duration;
        private float _stop_loss_point;
        private float _take_profit_point;

        public string figi { get { return _figi; } set { _figi = value; } }
        public string strat_name { get { return _strat_name; } set { _strat_name = value; } }
        public string strat_type { get { return _strat_type; } set { _strat_type = value; } }
        public string strat_duration { get { return _strat_duration; } set { _strat_duration = value; } }
        public float stop_loss_point { get { return _stop_loss_point; } set { _stop_loss_point = value; } }
        public float take_profit_point { get { return _take_profit_point; } set { _take_profit_point = value; } }
    }
}
