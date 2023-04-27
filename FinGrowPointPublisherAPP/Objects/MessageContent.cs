namespace FinGrowPointPublisherAPP.Objects
{
    public class MessageContent
    {
        private string _trade_id;
        private string _figi;
        private string _name;
        private string _ticker;
        private string _strat_name;
        private string _trade_type;
        private string _open_price;
        private string _target_price_1;
        private string _target_perc_1;
        private string _target_price_2;
        private string _target_perc_2;
        private string _stop_loss_price;
        private string _stop_loss_perc;
        private string _trade_dur_forecast;
        private string _communication_channel;
        private string _channel_id;
        private string _message_template_name;
        private string _final_message;
        private string _stop_loss_price_for_profit_2;
        private string _tinkoffAvailableShort;


        public string trade_id { get { return _trade_id; } set { _trade_id = value; } }
        public string figi { get { return _figi; } set { _figi = value; } }
        public string name { get { return _name; } set { _name = value; } }
        public string ticker { get { return _ticker; } set { _ticker = value; } }
        public string strat_name { get { return _strat_name; } set { _strat_name = value; } }
        public string trade_type { get { return _trade_type; } set { _trade_type = value; } }
        public string open_price { get { return _open_price; } set { _open_price = value; } }
        public string target_price_1 { get { return _target_price_1; } set { _target_price_1 = value; } }
        public string target_perc_1 { get { return _target_perc_1; } set { _target_perc_1 = value; } }
        public string target_price_2 { get { return _target_price_2; } set { _target_price_2 = value; } }
        public string target_perc_2 { get { return _target_perc_2; } set { _target_perc_2 = value; } }
        public string stop_loss_price { get { return _stop_loss_price; } set { _stop_loss_price = value; } }
        public string stop_loss_perc { get { return _stop_loss_perc; } set { _stop_loss_perc = value; } }
        public string trade_dur_forecast { get { return _trade_dur_forecast; } set { _trade_dur_forecast = value; } }
        public string communication_channel { get { return _communication_channel; } set { _communication_channel = value; } }
        public string channel_id { get { return _channel_id; } set { _channel_id = value; } }
        public string message_template_name { get { return _message_template_name; } set { _message_template_name = value; } }
        public string final_message { get { return _final_message; } set { _final_message = value; } }
        public string stop_loss_price_for_profit_2 { get { return _stop_loss_price_for_profit_2; } set { _stop_loss_price_for_profit_2 = value; } }
        public string tinkoffAvailableShort { get { return _tinkoffAvailableShort; } set { _tinkoffAvailableShort = value; } }

    }
}