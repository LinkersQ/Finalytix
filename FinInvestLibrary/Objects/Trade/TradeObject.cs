using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        private float _tradeProfitByClose; // разница между ценой закрытия и открытия
        private float _tradeProfitByClosePerc; //относительная разница между ценой закрытия и открытия
        private float _tradeProfitByMax;
        private float _tradeProfitByMaxPerc;
        private float _tradeProfitByMin;
        private float _tradeProfitByMinPerc;
        
        private TimeSpan _tradeDuration;

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
        public float tradeProfitByClose { get { return _tradeProfitByClose; } set { _tradeProfitByClose = value; } }
        public float tradeProfitByClosePerc { get { return _tradeProfitByClosePerc; } set { _tradeProfitByClosePerc = value; } }
        public float tradeProfitByMax { get { return _tradeProfitByMax; } set { _tradeProfitByMax = value; } }
        public float tradeProfitByMaxPerc { get { return _tradeProfitByMaxPerc; } set { _tradeProfitByMaxPerc = value; } }
        public float tradeProfitByMin { get { return _tradeProfitByMin; } set { _tradeProfitByMin = value; } }
        public float tradeProfitByMinPerc { get { return _tradeProfitByMinPerc; } set { _tradeProfitByMinPerc = value; } }
        public TimeSpan tradeDuration { get { return _tradeDuration; } set { _tradeDuration = value; } }

    }
}
