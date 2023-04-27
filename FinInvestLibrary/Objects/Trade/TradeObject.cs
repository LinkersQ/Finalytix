using Tinkoff.InvestApi.V1;

namespace FinInvestLibrary.Objects.Trade
{
    public class TradeObject
    {
        private string _tradeId;
        private string _tradeType;
        private string _stratName;
        private int _openCandleId;
        private int _closeCandleId;
        private DateTime _openCandleDt;
        private DateTime _closeCandleDt;
        private string _figi;
        private DateTime _tradeStartDt;
        private DateTime _tradeCloseDt;
        private float _openTradePrice; //цена входа в сделку
        private float _closeTradePrice; //цена выхода из сделки согласно стратегии (по технике)
        private float _maxTradePrice; //максимальная цена актива в сделке
        private int _maxtradepricecandleid;
        private DateTime _maxtradepricecandledt;
        private float _minTradePrice; // минимальная цена актива в сделке
        private int _mintradepricecandleid;
        private DateTime _mintradepricecandledt;

        private float _tradeDuration;
        //--15.03.2023
        private string _calculatetype;

        //16.03.2023
        private float _target1Value;
        private float _target2Value;
        private float _stopLoss1Value;
        private float _stopLoss2Value;

        private float _target1ClosePrice;
        private DateTime _target1CloseDT;
        private string _target1CloseCause;

        private float _target2ClosePrice;
        private DateTime _target2CloseDT;
        private string _target2CloseCause;

        //17.03.2023
        private bool _trade_is_close_analytic; //статус сделки для аналитической логики
        private bool _trade_is_close_communication; //статус сделки для публикуемых сделок
        
        //27.04.2023
        private ShareObject _shareObject;



        public int maxtradepricecandleid { get { return _maxtradepricecandleid; } set { _maxtradepricecandleid = value; } }
        public DateTime maxtradepricecandledt { get { return _maxtradepricecandledt; } set { _maxtradepricecandledt = value; } }
        public int mintradepricecandleid { get { return _mintradepricecandleid; } set { _mintradepricecandleid = value; } }
        public DateTime mintradepricecandledt { get { return _mintradepricecandledt; } set { _mintradepricecandledt = value; } }

        public string tradeId { get { return _tradeId; } set { _tradeId = value; } }
        public string tradeType { get { return _tradeType; } set { _tradeType = value; } }
        public string stratName { get { return _stratName; } set { _stratName = value; } }
        public int openCandleId { get { return _openCandleId; } set { _openCandleId = value; } }
        public int closeCandleId { get { return _closeCandleId; } set { _closeCandleId = value; } }
        public DateTime openCandleDt { get { return _openCandleDt; } set { _openCandleDt = value; } }
        public DateTime closeCandleDt { get { return _closeCandleDt; } set { _closeCandleDt = value; } }
        public string figi { get { return _figi; } set { _figi = value; } }
        public DateTime tradeStartDt { get { return _tradeStartDt; } set { _tradeStartDt = value; } }
        public DateTime tradeCloseDt { get { return _tradeCloseDt; } set { _tradeCloseDt = value; } }
        public float openTradePrice { get { return _openTradePrice; } set { _openTradePrice = value; } }
        public float closeTradePrice { get { return _closeTradePrice; } set { _closeTradePrice = value; } }
        public float maxTradePrice { get { return _maxTradePrice; } set { _maxTradePrice = value; } }
        public float minTradePrice { get { return _minTradePrice; } set { _minTradePrice = value; } }
        public float tradeDuration { get { return _tradeDuration; } set { _tradeDuration = value; } }
        //--15.03.2023
        public string calculatetype { get { return _calculatetype; } set { _calculatetype = value; } }


        //16.03.2023
        public float target1Value { get { return _target1Value; } set { _target1Value = value; } }
        public float target2Value { get { return _target2Value; } set { _target2Value = value; } }
        public float stopLoss1Value { get { return _stopLoss1Value; } set { _stopLoss1Value = value; } }
        public float stopLoss2Value { get { return _stopLoss2Value; } set { _stopLoss2Value = value; } }

        public float target1ClosePrice { get { return _target1ClosePrice; } set { _target1ClosePrice = value; } }
        public DateTime target1CloseDT { get { return _target1CloseDT; } set { _target1CloseDT = value; } }
        public string target1CloseCause { get { return _target1CloseCause; } set { _target1CloseCause = value; } }

        public float target2ClosePrice { get { return _target2ClosePrice; } set { _target2ClosePrice = value; } }
        public DateTime target2CloseDT { get { return _target2CloseDT; } set { _target2CloseDT = value; } }
        public string target2CloseCause { get { return _target2CloseCause; } set { _target2CloseCause = value; } }

        //17.03.2023
        public bool trade_is_close_analytic { get { return _trade_is_close_analytic; } set { _trade_is_close_analytic = value; } }
        public bool trade_is_close_communication { get { return _trade_is_close_communication; } set { _trade_is_close_communication = value; } }

        //27.04.2023
        public ShareObject shareObject { get { return _shareObject; } set { _shareObject = value; } }

    }
}
