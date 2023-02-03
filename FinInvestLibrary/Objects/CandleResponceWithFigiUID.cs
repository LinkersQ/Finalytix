using Tinkoff.InvestApi.V1;

namespace FinInvestLibrary.Objects
{
    public class CandleResponceWithFigiUID
    {
        private string _Figi;
        private string _Uid;

        private GetCandlesResponse _CandlesResponse;

        public string Figi { get { return _Figi; } set { _Figi = value; } }
        public string Uid { get { return _Uid; } set { _Uid = value; } }
        public GetCandlesResponse CandlesResponse { get { return _CandlesResponse; } set { _CandlesResponse = value; } }
    }
}
