namespace FinInvestLibrary.Functions.Mathematica
{
    public class MathMACD
    {
        /// <summary>
        /// Возвращает значение Alpha для заданного интервала 
        /// </summary>
        /// <param name="duration">Задает интервал, для которого необходимо произвести расчет Alpha</param>
        /// <returns></returns>
        public float getAlphaValueForDuration(int duration)
        {
            float returnValue;
            float alpha = 2 / ((float)duration + 1);
            returnValue = alpha;
            return returnValue;
        }

        public float getEMAValue(float alphaValue, FinInvestLibrary.Objects.Candle candle, float prevEMAValue)
        {
            float returnValue;

            returnValue = (float)(alphaValue * candle.close_price + (1 - alphaValue) * prevEMAValue);

            return returnValue;
        }

        public float getEMAValue(float alphaValue, float currentValue, float prevEMAValue)
        {
            float returnValue;

            returnValue = (float)(alphaValue * currentValue + (1 - alphaValue) * prevEMAValue);

            return returnValue;
        }

        /// <summary>
        /// Рассчитывает занчение MACD как разницу между первым и вторым значением
        /// </summary>
        /// <param name="firstValue"></param>
        /// <param name="secondValue"></param>
        /// <returns>Вычисленное значение разности между firstValue и secondValue</returns>
        public float getMACDValue(float firstValue, float secondValue)
        {
            float returnValue = firstValue - secondValue;
            return returnValue;
        }
    }
}
