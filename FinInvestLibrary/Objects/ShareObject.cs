using FinInvestLibrary.Objects.Trade;

namespace FinInvestLibrary.Objects
{
    public class ShareObject
    {
        private string? _figi;
        private string? _ticker;
        private string? _class_code;
        private string? _isin;
        private int? _lot;
        private string? _currency;
        private float? _klong;
        private float? _kshort;
        private float? _dlong;
        private float? _dshort;
        private float? _dlong_min;
        private float? _dshort_min;
        private bool? _short_enabled_flag;
        private string? _name;
        private string? _exchange;
        private DateTimeOffset? _ipo_date;
        private Int64? _issue_size;
        private string? _country_of_risk;
        private string? _country_of_risk_name;
        private string? _sector;
        private Int64? _issue_size_plan;
        private float? _nominal;
        private string? _trading_status;
        private bool? _otc_flag;
        private bool? _buy_available_flag;
        private bool? _sell_available_flag;
        private bool? _div_yield_flag;
        private string? _share_type;
        private float? _min_price_increment;
        private bool? _api_trade_available_flag;
        private string? _uid;
        private string? _real_exchange;
        private string? _position_uid;
        private bool? _for_iis_flag;
        private bool? _for_qual_investor_flag;
        private bool? _weekend_flag;
        private bool? _blocked_tca_flag;
        private DateTimeOffset? _first_1min_candle_date;
        private DateTimeOffset? _first_1day_candle_date;

        

        private int _lastCandleIdForStrategy;
        private List<CandleForSMAStratAnalysis> _candleForSMAStratAnalysisList;
        private List<TradeObject> _tradeObjects;

        private bool _UnavailableForAnalysys;

        //04.05.2023 - Добавлена коллекция свечей для работы функционала по расчету MA EMA MACD
        private List<Candle> _candleObjects;
        public List<Candle> candleObjects { get { return _candleObjects; } set { _candleObjects = value; } }


        public string figi { get { return _figi; } set { _figi = value; } }
        public string ticker { get { return _ticker; } set { _ticker = value; } }
        public string class_code { get { return _class_code; } set { _class_code = value; } }
        public string isin { get { return _isin; } set { _isin = value; } }
        public int lot { get { return (int)_lot; } set { _lot = value; } }
        public string currency { get { return _currency; } set { _currency = value; } }
        public float klong { get { return (float)_klong; } set { _klong = value; } }
        public float kshort { get { return (float)_kshort; } set { _kshort = value; } }
        public float dlong { get { return (float)_dlong; } set { _dlong = value; } }
        public float dshort { get { return (float)_dshort; } set { _dshort = value; } }
        public float dlong_min { get { return (float)_dlong_min; } set { _dlong_min = value; } }
        public float dshort_min { get { return (float)_dshort_min; } set { _dshort_min = value; } }
        public bool short_enabled_flag { get { return (bool)_short_enabled_flag; } set { _short_enabled_flag = value; } }
        public string name { get { return _name; } set { _name = value; } }
        public string exchange { get { return _exchange; } set { _exchange = value; } }
        public DateTimeOffset ipo_date { get { return (DateTimeOffset)_ipo_date; } set { _ipo_date = value; } }
        public Int64 issue_size { get { return (long)_issue_size; } set { _issue_size = value; } }
        public string country_of_risk { get { return _country_of_risk; } set { _country_of_risk = value; } }
        public string country_of_risk_name { get { return _country_of_risk_name; } set { _country_of_risk_name = value; } }
        public string sector { get { return _sector; } set { _sector = value; } }
        public Int64 issue_size_plan { get { return (long)_issue_size_plan; } set { _issue_size_plan = value; } }
        public float nominal { get { return (float)_nominal; } set { _nominal = value; } }
        public string trading_status { get { return _trading_status; } set { _trading_status = value; } }
        public bool otc_flag { get { return (bool)_otc_flag; } set { _otc_flag = value; } }
        public bool buy_available_flag { get { return (bool)_buy_available_flag; } set { _buy_available_flag = value; } }
        public bool sell_available_flag { get { return (bool)_sell_available_flag; } set { _sell_available_flag = value; } }
        public bool div_yield_flag { get { return (bool)_div_yield_flag; } set { _div_yield_flag = value; } }
        public string share_type { get { return _share_type; } set { _share_type = value; } }
        public float min_price_increment { get { return (float)_min_price_increment; } set { _min_price_increment = value; } }
        public bool api_trade_available_flag { get { return (bool)_api_trade_available_flag; } set { _api_trade_available_flag = value; } }
        public string uid { get { return _uid; } set { _uid = value; } }
        public string real_exchange { get { return _real_exchange; } set { _real_exchange = value; } }
        public string position_uid { get { return _position_uid; } set { _position_uid = value; } }
        public bool for_iis_flag { get { return (bool)_for_iis_flag; } set { _for_iis_flag = value; } }
        public bool for_qual_investor_flag { get { return (bool)_for_qual_investor_flag; } set { _for_qual_investor_flag = value; } }
        public bool weekend_flag { get { return (bool)_weekend_flag; } set { _weekend_flag = value; } }
        public bool blocked_tca_flag { get { return (bool)_blocked_tca_flag; } set { _blocked_tca_flag = value; } }
        public DateTimeOffset first_1min_candle_date { get { return (DateTimeOffset)_first_1min_candle_date; } set { _first_1min_candle_date = value; } }
        public DateTimeOffset first_1day_candle_date { get { return (DateTimeOffset)_first_1day_candle_date; } set { _first_1day_candle_date = value; } }
        public int LastCandleIdForStrategy { get { return _lastCandleIdForStrategy; } set { _lastCandleIdForStrategy = value; } }
        public List<CandleForSMAStratAnalysis> candleForSMAStratAnalysisList { get { return _candleForSMAStratAnalysisList; } set { _candleForSMAStratAnalysisList = value; } }
        public List<TradeObject> tradeObjects { get { return _tradeObjects; } set { _tradeObjects = value; } }
        public bool UnavailableForAnalysys { get { return _UnavailableForAnalysys; } set { _UnavailableForAnalysys = value; } }
    }
}
