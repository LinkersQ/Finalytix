using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FinInvestLibrary.Functions.Mathematica
{
    public class MathEMA
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
    }
}
