namespace FinInvestLibrary.Objects.StrategyObjects
{
    public class StrategyConfigurationObject
    {
        private string _PAR1_TYPE;
        private string _PAR1_DURATION;
        private string _PAR2_TYPE;
        private string _PAR2_DURATION;
        private string _STRAT_NAME;
        private string _STRAT_TYPE;
        private string _STRAT_NAME_FOR_BA;
        private string _STRAT_DURATION_FOR_BA;
        private string _STRAT_COMMUNICATION_CHANNEL_ID;
        private string _STRAT_COMMUNICATION_TEMPLATE;
        private string _APP_CONFIGURATION;

        public string PAR1_TYPE { get { return _PAR1_TYPE; } set { _PAR1_TYPE = value; } }
        public string PAR1_DURATION { get { return _PAR1_DURATION; } set { _PAR1_DURATION = value; } }
        public string PAR2_TYPE { get { return _PAR2_TYPE; } set { _PAR2_TYPE = value; } }
        public string PAR2_DURATION { get { return _PAR2_DURATION; } set { _PAR2_DURATION = value; } }
        public string STRAT_NAME { get { return _STRAT_NAME; } set { _STRAT_NAME = value; } }
        public string STRAT_TYPE { get { return _STRAT_TYPE; } set { _STRAT_TYPE = value; } }
        public string STRAT_NAME_FOR_BA { get { return _STRAT_NAME_FOR_BA; } set { _STRAT_NAME_FOR_BA = value; } }
        public string STRAT_DURATION_FOR_BA { get { return _STRAT_DURATION_FOR_BA; } set { _STRAT_DURATION_FOR_BA = value; } }
        public string STRAT_COMMUNICATION_CHANNEL_ID { get { return _STRAT_COMMUNICATION_CHANNEL_ID; } set { _STRAT_COMMUNICATION_CHANNEL_ID = value; } }
        public string STRAT_COMMUNICATION_TEMPLATE { get { return _STRAT_COMMUNICATION_TEMPLATE; } set { _STRAT_COMMUNICATION_TEMPLATE = value; } }
        public string APP_CONFIGURATION { get { return _APP_CONFIGURATION; } set { _APP_CONFIGURATION = value; } }
    }
}
