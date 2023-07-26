using FinInvestLibrary.Objects.Trade;
using Newtonsoft.Json;

namespace FinInvestLibrary.Functions.LocalOperations
{
    public class PrepareData
    {
        public string JSONSerializedTrade(TradeObject tradeObject)
        {
            string target_perc_1_str = (tradeObject.target1Value * 100).ToString();
            string target_perc_2_str = (tradeObject.target2Value * 100).ToString();

            string stop_loss_perc_str;
            if (tradeObject.target1CloseCause.Equals("STOPLOSS_1"))
            {
                stop_loss_perc_str = (tradeObject.stopLoss1Value * 100).ToString();
            }
            else
            { 
                stop_loss_perc_str = ((tradeObject.target1Value) / 2 * 100).ToString(); 
            }

            if (stop_loss_perc_str.Length > 5)
            {
                stop_loss_perc_str = stop_loss_perc_str.Substring(0, 5);
            }

            if (target_perc_1_str.Length > 5)
            {
                target_perc_1_str = target_perc_1_str.Substring(0, 5);
            }

            if (target_perc_2_str.Length > 5)
            {
                target_perc_2_str = target_perc_2_str.Substring(0, 5);
            }

            //Готовим объект для сериализации в JSON
            var obj = new
            {
                trade_id = tradeObject.tradeId
                ,
                figi = tradeObject.shareObject.figi
                ,
                name = tradeObject.shareObject.name
                ,
                ticker = "#" + tradeObject.shareObject.ticker
                ,
                strat_name = tradeObject.stratName
                ,
                trade_type = tradeObject.tradeType
                ,
                open_price = tradeObject.openTradePrice.ToString()
                ,
                target_price_1 = tradeObject.tradeType.Equals("LONG") ?
                            (tradeObject.openTradePrice + (tradeObject.target1Value * tradeObject.openTradePrice)).ToString()
                                : (tradeObject.openTradePrice - (tradeObject.target1Value * tradeObject.openTradePrice)).ToString()
                ,
                target_perc_1 = target_perc_1_str
                ,
                target_price_2 = tradeObject.tradeType.Equals("LONG") ?
                            (tradeObject.openTradePrice + (tradeObject.target2Value * tradeObject.openTradePrice)).ToString()
                                : (tradeObject.openTradePrice - (tradeObject.target2Value * tradeObject.openTradePrice)).ToString()
                ,
                target_perc_2 = target_perc_2_str
                ,
                stop_loss_price = tradeObject.tradeType.Equals("LONG") ?
                            (tradeObject.openTradePrice + (tradeObject.stopLoss1Value * tradeObject.openTradePrice)).ToString()
                            : (tradeObject.openTradePrice - (tradeObject.stopLoss1Value * tradeObject.openTradePrice)).ToString()
                ,
                stop_loss_perc = stop_loss_perc_str //если сработал стоплосс 2 - доходность расcчитывается как таргет профита 1 деленное на 2.
                ,
                trade_dur_forecast = tradeObject.tradeDuration.ToString()
                ,
                communication_channel = "Telegram".ToUpper()
                ,
                channel_id = tradeObject.communication_channel_id
                ,
                message_template_name = tradeObject.communication_template
                ,
                stop_loss_price_for_profit_2 = tradeObject.openTradePrice.ToString()
                ,
                tinkoffAvailableShort = tradeObject.shareObject.short_enabled_flag is true ? "Разрешен" : "Запрещен"
                ,
                trade_open_date = tradeObject.tradeStartDt.ToString("HH:mm dd.MM.yy")

            };
            string jsonObj = JsonConvert.SerializeObject(obj);
            return jsonObj;
        }
    }
}
