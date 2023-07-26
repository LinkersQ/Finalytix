using FinInvestLibrary.Functions.LocalOperations;
using FinInvestLibrary.Functions.Mathematica;
using FinInvestLibrary.Objects;
using FinInvestLibrary.Objects.Calculations;
using log4net;

namespace TA.Metrics.Calculater
{
    internal class Program
    {
        public static readonly ILog log = LogManager.GetLogger(typeof(Program));

        static void Main(string[] args)
        {
            string duration, connectionString, scale, appConfiguration, calcType;
            log4net.Config.XmlConfigurator.Configure();

#if DEBUG
            appConfiguration = "HistoryCalculate";
            duration = "1226";
            scale = "1_day_scale";
            calcType = "MACD_SL";
#else
            duration = args[0];
            scale = args[1];
            appConfiguration = args[2];
            calcType = args[3];
#endif
            string appPath = Environment.CurrentDirectory;
            string connectionStringPath = appPath + "\\connectionString.txt";
            connectionString = File.ReadAllText(connectionStringPath);

            Console.WriteLine(DateTime.Now.AddDays(-3).DayOfWeek.ToString());

            List<string> durationsList = duration.Split(',').ToList();
            List<string> scalesList = scale.Split(',').ToList();

            log.Info("Текущая конфигурация:");
            log.Info("\tДлительности для расчета:");
            foreach (var str in durationsList)
            { log.Info("\t\t" + str); }
            log.Info("\tМасштабы для расчета:");
            foreach (var str in scalesList)
            { log.Info("\t\t" + str); }

            log.Info("Приложение запущено в режиме " + appConfiguration);
            log.Info("Расчет производится в режиме " + calcType);

            if (calcType == "MA")
            {
                MACalc(connectionString, scale, appConfiguration, calcType, durationsList);
            }
            else if (calcType == "EMA")
            {
                EMACalc(connectionString, scale, appConfiguration, calcType, durationsList);
            }
            else if (calcType == "MACD")
            {
                MACDCalc(connectionString, scale, appConfiguration, calcType, durationsList);
            }
            else if (calcType == "MACD_SL")
            {
                MACD_SLCalc(connectionString, scale, appConfiguration, calcType, durationsList);
            }
            else
            {
                log.Error("Невалидный параметр calcType");
            }
        }

        /// <summary>
        /// Логика рассчета сигнальной линии MACD (MACD SL)
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scale"></param>
        /// <param name="appConfiguration"></param>
        /// <param name="calcType"></param>
        /// <param name="durationsList"></param>
        /// <exception cref="NotImplementedException"></exception>
        private static void MACD_SLCalc(string connectionString, string scale, string appConfiguration, string calcType, List<string> durationsList)
        {

            if (appConfiguration == "HistoryCalculate")
            {
                CalculateMACD_SLForAllNotMACDCandles(connectionString, scale, calcType, durationsList);
            }
            else if (appConfiguration == "RegularCalculate")
            {
                CalculateMACD_SLForOpenCandles(connectionString, scale, calcType, durationsList);
            }
            else
            {
                log.Error("Невалидный параметр appConfiguration");
            }
        }

        /// <summary>
        /// Рассчет MACD SL для массива открытых свечей
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scale"></param>
        /// <param name="calcType"></param>
        /// <param name="durationsList"></param>
        /// <exception cref="NotImplementedException"></exception>
        private static void CalculateMACD_SLForOpenCandles(string connectionString, string scale, string calcType, List<string> durationsList)
        {
            /// Все выборки выполняются только для свечей, которые не равны субботе или восскресенью
            /// 0) Получаем выборку свечей, которые на текущий момент являются OPEN
            /// 1) Убедиться что все компоненты для рассчета существуют
            ///     а) Открытая свеча
            ///     б) Рассчет MACD SL за предыдущий день
            ///     в) Рассчет MACD за текущий день
            ///    Если компонента одного из компонентов не сущестует - завершаем алгоритм и записываем в лог сообщение об ошибке. 
            /// 2) Произвести рассчет MACD SL
            /// 3) Проверить существование рассчета MACD SL за текущий день (по признаку открытой свечи)
            ///     а) Если рассчет существует - вызвать функцию UpdateCalculationsTable()
            ///     б) Если рассчет НЕ существует - вызвать функцию InsertIntoCalculationsTable()
            /// 

            foreach (var duration in durationsList)
            {
                log.Info("Запущен рассчет по длительности " + duration.ToString());
                log.Info("Выполняю проверки:");
                log.Info("Проверка 1: проверяется наличие открытых свечей");
                var openCanldes = new PgExecuter(connectionString, log).GetOpenCandles(scale);
                if (openCanldes.Count > 0)
                {
                    log.Info("Проверка пройдена.");
                    log.Info("Найдено " + openCanldes.Count + " открытых свечей.");

                    log.Info("Проверка 2: проверяется наличие предыдущего расчета MACD SL.");

                    List<CalculationObject> prevCalculations = new List<CalculationObject>();//список для полученных рассчетов MACD_SL за предыдущий день
                    foreach (var candle in openCanldes)
                    {
                        var calc = new PgExecuter(connectionString, log).GetPreviousCalculation(candle, calcType, "9" + duration);
                        if (calc == null)
                        {
                            candle.isDelete = true;
                        }
                        else
                        {
                            calc.next_candle_id = candle.id;
                            prevCalculations.Add(calc);
                        }

                    }
                    if (prevCalculations.Count > 0)
                    {
                        log.Info("Проверка выполнена.");


                        log.Info("Проверка 3: проверяется наличия текущего MACD рассчета");

                        List<CalculationObject> currentCalculations = new List<CalculationObject>();
                        foreach (var candle in openCanldes)
                        {
                            var calc = new PgExecuter(connectionString, log).GeCurrentCalculation(candle, "MACD", duration);
                            if (calc == null)
                            {
                                candle.isDelete = true;
                            }
                            else
                            {
                                currentCalculations.Add(calc);
                            }
                        }
                        if (currentCalculations.Count > 0)
                        {
                            log.Info("Открытых свечей: " + openCanldes.Count);
                            log.Info("Предыдущих рассчетов " + calcType + ": " + prevCalculations.Count);
                            log.Info("Текущих рассчетов  MACD: " + currentCalculations.Count);

                            log.Info("Произвожу рассчет MACD SL за текущий день");

                            var candlesForCalc = openCanldes.Where(w => w.isDelete == false).ToList();

                            log.Info("К рассчету допущены " + candlesForCalc.Count.ToString());

                            foreach (var candle in candlesForCalc)
                            {

                                var alpha = new MathMACD().getAlphaValueForDuration(9);
                                var prevSLValue = prevCalculations.FirstOrDefault(f => f.next_candle_id == candle.id);
                                var currMACDValue = currentCalculations.FirstOrDefault(f => f.candle_id == candle.id);

                                if (prevSLValue != null && currMACDValue != null)
                                {
                                    var sl = new MathMACD().getEMAValue(alpha, currMACDValue.value, prevSLValue.value);

                                    CalculationObject calculationObject = new CalculationObject();
                                    calculationObject.candle_id = candle.id;
                                    calculationObject.figi = candle.figi;
                                    calculationObject.candle_scale = candle.scale;
                                    calculationObject.value = sl;
                                    calculationObject.calc_type = calcType;
                                    calculationObject.duration = Convert.ToInt32("9" + duration);
                                    calculationObject.insertdate = DateTime.Now;
                                    calculationObject.updatedate = DateTime.Now;

                                    log.Info("Рассчет произведен");
                                    log.Info("Для свечи candle_id = " + candle.id + " значение MACD SL = " + sl);

                                    log.Info("Проверяю наличие существующего рассчета MACD SL для свечи candle_id = " + candle.id);
                                    var existCalc = new PgExecuter(connectionString, log).GeCurrentCalculation(candle, calcType, "9" + duration);

                                    if (existCalc != null)
                                    {

                                        log.Info("Рассчет существует. Обновляю существующую строку");
                                        existCalc.value = calculationObject.value;
                                        existCalc.updatedate = calculationObject.updatedate;
                                        var updateRes = new PgExecuter(connectionString, log).UpdateCalculationsTable(existCalc);
                                    }
                                    else
                                    {
                                        log.Info("Рассчет не существует. Добавляю новую строку");
                                        var insertRes = new PgExecuter(connectionString, log).InsertIntoCalculationsTable(calculationObject);
                                    }
                                }
                                else
                                {
                                    log.Info("Для рассчета по свече candle_id = " + candle.id + " не хватает данных");
                                }
                            }
                        }
                        else
                        {
                            log.Info("Проверка 3 не пройдена. Завершаю работу.");
                        }

                    }
                    else
                    {
                        log.Info("Проверка 2 не пройдена. Завершаю работу.");
                    }




                }
                else
                {
                    log.Info("Проверка 1 не пройдена. Завершаю работу.");
                }

            }


        }


        /// <summary>
        /// Рассчет MACD SL для массива закрытых свечей
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scale"></param>
        /// <param name="calcType"></param>
        /// <param name="durationsList"></param>
        private static void CalculateMACD_SLForAllNotMACDCandles(string connectionString, string scale, string calcType, List<string> durationsList)
        {
            /// 1) Запрашиваем MACD по перечисленным масштабам (выкидываем выходные дни)
            /// 2) Проверяем наличие существующих рассчетов MACD для перечисленных массштабов
            ///     а) если масштаба нет - выводим сообщение об отсутсвии такового и переходим к следующему масштабу
            ///     б) если масштаб есть - переходим к шагу 3
            /// 3) Проверяем наличие уже рассчитанного SL по масштабу
            ///     а) если рассчет существуем - переходим к следующему
            ///     б) если рассчета нет - вычисляем MA SL (сглаживание 9)
            ///         a) сохраняем рассчет MA в таблицу с calc_type = MA_SL и duration = 9
            ///     в) после рассчета MA SL вычисляем EMA SL (Для первого входа используем значение MA) сглаживание = 9
            ///         а) сохраняем рассчет EMA SL (сглаживание 9)
            ///         
            foreach (string duration in durationsList)
            {
                log.Info("Запрашиваю MACD вычисления для длительности " + duration);
                var macdCalculations = new PgExecuter(connectionString, log).GetCalculations("MACD", duration, scale, true);
                log.Info("Получено " + macdCalculations.Count + " готовых вычислений");
                macdCalculations = macdCalculations.OrderBy(o => o.candle_id).ToList();
                log.Info("Проверяю наличие существующих вычислений SL");

                List<CalculationObject> calculationsMACD_MA_SL = new List<CalculationObject>();
                foreach (var calc in macdCalculations)
                {
                    log.Info("Проверяю наличие существующего рассчета EMA(9) SL(" + duration + ") для свечи " + calc.candle_id);
                    CalculationObject ema_sl = new PgExecuter(connectionString, log).GetCalculations(calc.candle_id.ToString(), "EMA_SL", "9", scale);
                    if (ema_sl == null)
                    {
                        log.Info("Вычисления не найдены.");
                        log.Info("Вычисляю MA SL для свечи " + calc.candle_id);
                        log.Info("Получаю MACD вычисления для рассчета MA SL 9");
                        var res = macdCalculations.Where(w => w.candle_id <= calc.candle_id & w.figi == calc.figi).OrderByDescending(od => od.candle_id).Take(9).ToList();
                        if (res.Count() == 9)
                        {

                            float[] macdValues = new float[res.Count()];
                            for (int i = 0; i < res.Count(); i++)
                            {
                                macdValues[i] = res[i].value;
                            }

                            var MACD_MA_SL = new MathMA(log).CalcMA(macdValues);
                            var calculationObj = new CalculationObject();
                            calculationObj.candle_id = calc.candle_id;
                            calculationObj.figi = calc.figi;
                            calculationObj.value = MACD_MA_SL;
                            calculationObj.calc_type = "MA_SL";

                            calculationsMACD_MA_SL.Add(calculationObj);

                            log.Info("MA SL = " + MACD_MA_SL);

                        }
                        else
                        {
                            log.Info("Недостаточно данных для вычисления MA_SL");
                        }

                    }
                    else
                    {
                        log.Info("Вычисление EMA SL для свечи " + ema_sl.candle_id + " существует. Значение = " + ema_sl.value);
                    }

                }
                log.Info("Вычисление MA SL для " + duration + " завершено. Сформирован массив вычислений, содержащий " + calculationsMACD_MA_SL.Count + " объектов");
                log.Info("Приступаю к вычислению EMA SL для длительности " + duration);

                List<CalculationObject> calculationsEMA_SL_list = new List<CalculationObject>();//список для сохранения рассчетов по EMA(9)_SL

                foreach (var calcMA_SL in calculationsMACD_MA_SL)
                {
                    log.Info("Вычисляю EMA(9) SL для свечи " + calcMA_SL.candle_id);

                    log.Info("Проверяю наличие значения EMA SL для свечи предшествующей candle_id = " + calcMA_SL.candle_id);
                    var prevEMA_SL = calculationsEMA_SL_list.Where(w => w.candle_id < calcMA_SL.candle_id & w.figi == calcMA_SL.figi).OrderByDescending(od => od.candle_id).Take(1).ToList();
                    if (prevEMA_SL != null)
                    {
                        if (prevEMA_SL.Count == 1)
                        {
                            log.Info("Предыдущая свеча найдена: candle_id = " + prevEMA_SL[0].id);

                            //Вычисляю EMA по MA
                            var alpha = new MathMACD().getAlphaValueForDuration(9);
                            var emaSLValue = new MathMACD().getEMAValue(alpha, calcMA_SL.value, prevEMA_SL[0].value);

                            var ema_calculationObj = new CalculationObject();
                            ema_calculationObj.candle_id = calcMA_SL.candle_id;
                            ema_calculationObj.figi = calcMA_SL.figi;
                            ema_calculationObj.value = emaSLValue;
                            ema_calculationObj.calc_type = calcType;
                            ema_calculationObj.duration = Convert.ToInt32("9" + duration);
                            ema_calculationObj.insertdate = DateTime.Now;
                            ema_calculationObj.updatedate = DateTime.Now;
                            ema_calculationObj.candle_scale = scale;

                            calculationsEMA_SL_list.Add(ema_calculationObj);

                            log.Info("Рассчет EMA(9) SL по MA для свечи Candle _id = " + calcMA_SL.candle_id + " выполнен");
                            log.Info("Candle _id = " + calcMA_SL.candle_id + " EMA(9) SL = " + emaSLValue);

                            log.Info("Сохраняю полученный рассчет в репозитории вычислений");
                            var res = new PgExecuter(connectionString, log).InsertIntoCalculationsTable(ema_calculationObj);

                        }
                        else
                        {
                            //Если рассчет EMA(9) для предыдущей свечи не найден - рассчитываем EMA на основании рассчетов MA_SL
                            log.Info("Получаю предыдущее значение MA SL");
                            var prevMA_SL = calculationsMACD_MA_SL.Where(w => w.candle_id < calcMA_SL.candle_id & w.figi == calcMA_SL.figi).OrderByDescending(od => od.candle_id).Take(1).ToList();
                            if (prevMA_SL != null)
                            {
                                if (prevMA_SL.Count == 1)
                                {
                                    log.Info("Предыдущая свеча найдена: candle_id = " + prevMA_SL[0].id);
                                    //Вычисляю EMA по MA
                                    var alpha = new MathMACD().getAlphaValueForDuration(9);
                                    var emaSLValue = new MathMACD().getEMAValue(alpha, calcMA_SL.value, prevMA_SL[0].value);

                                    var ema_calculationObj = new CalculationObject();
                                    ema_calculationObj.candle_id = calcMA_SL.candle_id;
                                    ema_calculationObj.figi = calcMA_SL.figi;
                                    ema_calculationObj.value = emaSLValue;
                                    ema_calculationObj.calc_type = calcType;
                                    ema_calculationObj.duration = Convert.ToInt32("9" + duration); ;
                                    ema_calculationObj.insertdate = DateTime.Now;
                                    ema_calculationObj.updatedate = DateTime.Now;
                                    ema_calculationObj.candle_scale = scale;

                                    calculationsEMA_SL_list.Add(ema_calculationObj);

                                    log.Info("Рассчет EMA(9) SL по MA для свечи Candle _id = " + calcMA_SL.candle_id + " выполнен");
                                    log.Info("Candle _id = " + calcMA_SL.candle_id + " EMA(9) SL = " + emaSLValue);

                                    log.Info("Сохраняю полученный рассчет в репозитории вычислений");
                                    var res = new PgExecuter(connectionString, log).InsertIntoCalculationsTable(ema_calculationObj);

                                }
                                else
                                {
                                    log.Error("Поиск предыдущей свечи вернул количество свечей отличное от 1. Текущее количество = " + prevMA_SL.Count);

                                }

                            }
                            else
                            {
                                log.Info("Предыдущая свеча не найдена. Перехожу к следующей свече");
                            }
                        }

                    }
                    else
                    {
                        //Если рассчет EMA(9) для предыдущей свечи не найден - рассчитываем EMA на основании рассчетов MA_SL
                        log.Info("Получаю предыдущее значение MA SL");
                        var prevMA_SL = calculationsMACD_MA_SL.Where(w => w.candle_id < calcMA_SL.candle_id & w.figi == calcMA_SL.figi).OrderByDescending(od => od.candle_id).Take(1).ToList();
                        if (prevMA_SL != null)
                        {
                            if (prevMA_SL.Count == 1)
                            {

                                log.Info("Предыдущая свеча найдена: candle_id = " + prevMA_SL[0].id);
                                //Вычисляю EMA по MA
                                var alpha = new MathMACD().getAlphaValueForDuration(9);
                                var emaSLValue = new MathMACD().getEMAValue(alpha, calcMA_SL.value, prevMA_SL[0].value);

                                var ema_calculationObj = new CalculationObject();
                                ema_calculationObj.candle_id = calcMA_SL.candle_id;
                                ema_calculationObj.figi = calcMA_SL.figi;
                                ema_calculationObj.value = emaSLValue;
                                ema_calculationObj.calc_type = calcType;
                                ema_calculationObj.duration = Convert.ToInt32("9" + duration); ;
                                ema_calculationObj.insertdate = DateTime.Now;
                                ema_calculationObj.updatedate = DateTime.Now;
                                ema_calculationObj.candle_scale = scale;

                                calculationsEMA_SL_list.Add(ema_calculationObj);

                                log.Info("Рассчет EMA(9) SL по MA для свечи Candle _id = " + calcMA_SL.candle_id + " выполнен");
                                log.Info("Candle _id = " + calcMA_SL.candle_id + " EMA(9) SL = " + emaSLValue);

                                log.Info("Сохраняю полученный рассчет в репозитории вычислений");
                                var res = new PgExecuter(connectionString, log).InsertIntoCalculationsTable(ema_calculationObj);

                            }
                            else
                            {
                                log.Error("Поиск предыдущей свечи вернул количество свечей отличное от 1. Текущее количество = " + prevMA_SL.Count);

                            }

                        }
                        else
                        {
                            log.Info("Предыдущая свеча не найдена. Перехожу к следующей свече");
                        }
                    }


                }
            }
        }

        /// <summary>
        /// Логика рассчета MACD
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scale"></param>
        /// <param name="appConfiguration"></param>
        /// <param name="calcType"></param>
        /// <param name="durationsList"></param>
        private static void MACDCalc(string connectionString, string scale, string appConfiguration, string calcType, List<string> durationsList)
        {


            if (appConfiguration == "HistoryCalculate")
            {
                CalculateMACDForAllNotMACDCandles(connectionString, scale, calcType, durationsList);
            }
            else if (appConfiguration == "RegularCalculate")
            {
                CalculateMACDForOpenCandles(connectionString, scale, calcType, durationsList);
            }

        }

        private static void CalculateMACDForOpenCandles(string connectionString, string scale, string calcType, List<string> durationsList)
        {

            ///
            /// 1) Проверка входных параметров
            ///     а) Только два значения
            ///     б) Первое значение меньше второго
            ///     в) Есть рассчет EMA по первому значению за текущий день в репозитории результатов вычислений
            ///     г) Есть рассчет EMA по второму значению за текущий день в репозитории результатов вычислений
            /// 
            /// 2) Проверяем наличие существующего рассчета по MACD для связки первого и второго значения
            /// 3) Если рассчета по MACD нет - вставляем новую строку
            /// 4) Если рассчет по MACD есть - проверяем отличие сохраненного VALUE от рассчитанного.
            ///     Если отличие есть - обновляем значение в репозитории 
            ///     Если отличий нет - обновление не проводим
            /// 
            log.Info("Проверяем наличие только двух значений в списке durations");
            if (durationsList.Count == 2)
            {
                log.Info("В durationList два значения: 1-ое = " + durationsList[0] + ", 2-ое = " + durationsList[1]);
                log.Info("Проверяю услоие: 1-ое значение < (меньше) 2-го");
                int firstDur = Convert.ToInt32(durationsList[0]);
                int secondDur = Convert.ToInt32(durationsList[1]);
                if (firstDur < secondDur)
                {
                    log.Info("Проверка пройдена");

                    log.Info("Проверяю наличие существующего рассчета EMA для первого и второго значения duration");
                    log.Info("Получаю рассчет метрик по EMA для первого значения в текущем дне (" + firstDur + ")");
                    var calcFirstValues = new PgExecuter(connectionString, log).GetCalculations("EMA", firstDur.ToString(), scale, false);
                    log.Info("Получаю рассчет метрик по EMA для второго значения в текущем дне (" + secondDur + ")");
                    var calcSecondValues = new PgExecuter(connectionString, log).GetCalculations("EMA", secondDur.ToString(), scale, false);
                    if (calcFirstValues.Count > 0 & calcSecondValues.Count > 0)
                    {
                        log.Info("Проверка выполнена");

                        log.Info("Получаю существующий MACD рассчет");
                        var calcMACDValue = new PgExecuter(connectionString, log).GetCalculations(calcType, firstDur.ToString() + secondDur.ToString(), scale, false);
                        log.Info("Рассчитываю MACD вставляя или обновляя записи в репозитории вычислений");
                        foreach (var firstEMACalc in calcFirstValues)
                        {
                            log.Info("Обрабатываю свечу " + firstEMACalc.candle_id);
                            var secondEMACalc = calcSecondValues.FirstOrDefault(f => f.candle_id == firstEMACalc.candle_id);
                            if (secondEMACalc != null)
                            {
                                float MACDValue = new MathMACD().getMACDValue(firstEMACalc.value, secondEMACalc.value);
                                var currMACDCalc = calcMACDValue.FirstOrDefault(f => f.candle_id == firstEMACalc.candle_id);
                                if (currMACDCalc != null)
                                {
                                    log.Info("Найден существующий рассчет MACD для свечи " + firstEMACalc.candle_id);
                                    log.Info("Проверяю отличие value в существующем рассчете MACD и нового рассчета");
                                    if (currMACDCalc.value == MACDValue)
                                    {
                                        log.Info("Значения существующего и нового рассчета MACD совпадают. Действий не требуется.");
                                    }
                                    else
                                    {
                                        currMACDCalc.value = MACDValue;
                                        log.Info("Значения существующего и нового рассчета MACD различаются. Обновляю значение MACD в репозитории для свечи " + firstEMACalc.candle_id);
                                        bool result = new PgExecuter(connectionString, log).UpdateCalculationsTable(currMACDCalc);
                                    }
                                }
                                else
                                {
                                    log.Info("Существующий MACD рассчет отсутсвует. Приступаю к созданию новой записи рассчета MACD.");
                                    var newMACDCalculations = new CalculationObject();
                                    newMACDCalculations.candle_id = firstEMACalc.candle_id;
                                    newMACDCalculations.figi = firstEMACalc.figi;
                                    newMACDCalculations.candle_scale = firstEMACalc.candle_scale;
                                    newMACDCalculations.calc_type = "MACD";
                                    newMACDCalculations.duration = Convert.ToInt32(firstEMACalc.duration.ToString() + secondEMACalc.duration.ToString());
                                    newMACDCalculations.value = MACDValue;
                                    newMACDCalculations.insertdate = DateTime.Now;
                                    newMACDCalculations.updatedate = DateTime.Now;

                                    bool result = new PgExecuter(connectionString, log).InsertIntoCalculationsTable(newMACDCalculations);
                                    if (result == false)
                                    {
                                        log.Error("Не удалось добавить новую запись MACD по свече " + newMACDCalculations.id);
                                    }
                                    else
                                    {
                                        log.Info("Новое вычисление MACD добавлено в репозиторий");
                                    }
                                }
                            }
                            else
                            {
                                log.Info("Нет готового рассчета по EMA для длительности " + secondDur.ToString());
                            }
                        }
                    }
                    else
                    {
                        log.Info("Проверка НЕ выполнена. Скорректируйте входные параметры и перезапустите приложение");
                    }
                }
                else
                {
                    log.Info("Проверка НЕ выполнена. Скорректируйте входные параметры и перезапустите приложение");
                }
            }
            else
            {
                log.Info("В duration лист количество значений <> 2. Скорректируйте входные параметры и перезапустите приложение");
            }
        }

        /// <summary>
        /// Рассчет MACD для массива закрытых свечей
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scale"></param>
        /// <param name="calcType"></param>
        /// <param name="durationsList"></param>
        private static void CalculateMACDForAllNotMACDCandles(string connectionString, string scale, string calcType, List<string> durationsList)
        {
            ///1) Проверяем, что в поле duration есть 
            ///     а) Только два значения
            ///     б) Первое значение меньше второго
            ///     в) Данные по первому значению рассчитаны в репозитории результатов вычислений (сразу получаем весь массив значений)
            ///     г) Данные по второму значению рассчитаны в репозитории результатов вычислений (сразу получаем весь массив значений)
            ///2) Определяем кол-во свечей, по которым могут быть рассчитаны значения
            ///3) Проводим рассчет индикатора MACD по доступным пересечениям
            ///
            ///4) Получаем свечи, по которым ранее не проводился рассчет MACD
            ///5) Пересекаем рассчет MACD и свечи из пункта 4 - на выходе получаем массив со свечами, по которым рассчитан MACD
            ///6) Записываем MACD вычисления в таблицу calculations
            ///
            log.Info("Проверяем наличие только двух значений в списке durations");
            if (durationsList.Count == 2)
            {
                log.Info("В durationList два значения: 1-ое = " + durationsList[0] + ", 2-ое = " + durationsList[1]);
                log.Info("Проверяю услоие: 1-ое значение < (меньше) 2-го");
                int firstDur = Convert.ToInt32(durationsList[0]);
                int secondDur = Convert.ToInt32(durationsList[1]);
                if (firstDur < secondDur)
                {
                    log.Info("Проверка пройдена");
                    log.Info("Получаю рассчет метрик по EMA для первого значения (" + firstDur + ")");
                    List<CalculationObject> firstDurEMACalculationsList = new PgExecuter(connectionString, log).GetCalculations("EMA", firstDur.ToString());
                    log.Info("Получаю рассчет метрик по EMA для второго значения (" + firstDur + ")");
                    List<CalculationObject> secondDurEMACalculationsList = new PgExecuter(connectionString, log).GetCalculations("EMA", secondDur.ToString());


                    log.Info("Проверяю условие: количество данных по первому и второму значениям > 0");
                    if (firstDurEMACalculationsList.Count > 0 & secondDurEMACalculationsList.Count > 0)
                    {
                        log.Info("Проверка пройдена");
                        log.Info("Количество метрик EMA для первого значения = " + firstDurEMACalculationsList.Count);
                        log.Info("Количество метрик EMA для второго значения = " + firstDurEMACalculationsList.Count);

                        var MACDObjects = firstDurEMACalculationsList.Join(secondDurEMACalculationsList,
                            f => f.candle_id,
                            s => s.candle_id,
                            (f, s) =>
                            new CalculationObject
                            {
                                candle_id = f.candle_id,
                                figi = f.figi,
                                candle_scale = f.candle_scale,
                                calc_type = "MACD",
                                duration = Convert.ToInt32(f.duration.ToString() + s.duration.ToString()),
                                value = new MathMACD().getMACDValue(f.value, s.value),
                                insertdate = DateTime.Now,
                                updatedate = DateTime.Now
                            }).ToList();

                        log.Info("Вычисление MACD завершено. Рассчитано " + MACDObjects.Count.ToString() + " объектов");
                        log.Info("Ищу ЗАКРЫТЫЕ свечи без расчета по " + calcType);
                        var candleObjects = new PgExecuter(connectionString, log).GetCandlesWithoutCalculation(calcType, scale, MACDObjects.First().duration.ToString());
                        log.Info("Получено " + candleObjects.Count.ToString() + " свечей без рассчета по MACD.");

                        log.Info("Проверяю наличие пересечений между MACD рассчетом и полученными свечами");
                        var MatchMACDCandleObjects = MACDObjects.Join(candleObjects,
                            macd => macd.candle_id,
                            candle => candle.id,
                            (macd, candle) => new CalculationObject
                            {
                                candle_id = macd.candle_id,
                                figi = macd.figi,
                                candle_scale = macd.candle_scale,
                                calc_type = macd.calc_type,
                                duration = macd.duration,
                                value = macd.value,
                                insertdate = macd.insertdate,
                                updatedate = macd.updatedate
                            }).ToList();


                        log.Info("В результате пересечения candles и MACD calculation получен массив, содержащий " + MatchMACDCandleObjects.Count.ToString() + " пересечений");

                        log.Info("Приступаю к сохранению полученных вычисление в таблицу calculations. Требуеся записать " + MatchMACDCandleObjects.Count.ToString());

                        int countOfSavedMacdCalc = 0;
                        foreach (var m in MatchMACDCandleObjects)
                        {
                            var result = new PgExecuter(connectionString, log).InsertIntoCalculationsTable(m);
                            countOfSavedMacdCalc++;
                        }
                        log.Info("Сохранено " + countOfSavedMacdCalc.ToString() + " MACD вычислений");

                    }

                }
                else
                {
                    log.Info("Проверка НЕ выполнена. Скорректируйте входные параметры и перезапустите приложение");
                }
            }
            else
            {
                log.Info("В duration лист количество значений <> 2. Скорректируйте входные параметры и перезапустите приложение");
            }
        }

        /// <summary>
        /// Логика рассчета EMA
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scale"></param>
        /// <param name="appConfiguration"></param>
        /// <param name="calcType"></param>
        /// <param name="durationsList"></param>
        private static void EMACalc(string connectionString, string scale, string appConfiguration, string calcType, List<string> durationsList)
        {
            var sharesList = new PgExecuter(connectionString, log).GetActualSharesList();
            log.Info("Список инструментов для расчета содержит " + sharesList.Count + " позиций");

            if (appConfiguration == "HistoryCalculate")
            {
                CalculateEMAForAllNotEMACandles(connectionString, scale, calcType, durationsList, sharesList);
            }
            else if (appConfiguration == "RegularCalculate")
            {
                CalculateEMAForOpenCandles(connectionString, scale, calcType, durationsList, sharesList);
            }
        }

        /// <summary>
        /// Рассчет EMA для массива открытых свечей
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scale"></param>
        /// <param name="calcType"></param>
        /// <param name="durationsList"></param>
        /// <param name="sharesList"></param>
        private static void CalculateEMAForOpenCandles(string connectionString, string scale, string calcType, List<string> durationsList, List<ShareObject> sharesList)
        {
            foreach (var share in sharesList)
            {
                log.Info("Ищу ОТКРЫТЫЕ свечи без расчета по " + calcType + " для актива " + share.name + "(" + share.figi + ")");

                foreach (var dur in durationsList)
                {
                    log.Info("\tПроверяю длительность: " + dur);
                    var candles = new PgExecuter(connectionString, log).GetOpenCandles(scale, share);
                    log.Info("\t\tНайдено " + candles.Count + " свечей");

                    log.Info("Расчитываю EMA по активу " + share.name + "(" + share.figi + "), длительность интервала расчета = " + dur);
                    foreach (var candle in candles)
                    {
                        log.Info("Для свечи candle_id = " + candle.id + " ищу предыдущее значение EMA");
                        float prevEMAValue = new PgExecuter(connectionString, log).GetPreviousValue(candle, dur, calcType);
                        if (prevEMAValue == -1)
                        {
                            log.Info("Для свечи candle_id = " + candle.id + " ищу предыдущее значение EMA не найдено.");
                            log.Info("Запустите TA.Metrics.Calculater в режиме \"HistoryCalculate\" или проверьте хранилище свечей");
                        }
                        else
                        {
                            //рассчитываем EMA с применением MA (для первого прохода)
                            float alphaValue = new MathMACD().getAlphaValueForDuration(Convert.ToInt32(dur));
                            log.Info("Рассчитана alpha = " + alphaValue);
                            float curEMAValue = new MathMACD().getEMAValue(alphaValue, candle, prevEMAValue);
                            log.Info("Рассчитана EMA = " + curEMAValue);
                            log.Info("Вставляю новую строку в calculations");

                            log.Info("Проверяю наличие предудущего расчета в таблице calculations  для свечи ID = " + candle.id);
                            int isExistMACalcForCandle = new PgExecuter(connectionString, log).CheckMACalculationForOpenCandle(candle, calcType, Convert.ToInt32(dur));
                            if (isExistMACalcForCandle > 0)
                            {
                                log.Info("Предыдущий расчет существует. Он будет обновлен на новое значение");
                                bool isUpdatetd = new PgExecuter(connectionString, log).UpdateCalculationsTable(candle, calcType, Convert.ToInt32(dur), curEMAValue.ToString());
                                if (isUpdatetd)
                                    log.Info("Успешно обновлено значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                                else
                                    log.Info("Не удалось обновить значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                            }
                            else
                            {
                                log.Info("Предыдущий расчет НЕ существует.Записываю результат по MA в таблицу calculations");
                                bool isInserted = new PgExecuter(connectionString, log).InsertIntoCalculationsTable(candle, calcType, Convert.ToInt32(dur), curEMAValue.ToString());
                                if (isInserted)
                                    log.Info("Успешно встравлено значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                                else
                                    log.Info("Не удалось вставить значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                            }



                        }
                    }
                }
            }
        }

        /// <summary>
        /// Рассчет EMA для массива закрытых свечей
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scale"></param>
        /// <param name="calcType"></param>
        /// <param name="durationsList"></param>
        /// <param name="sharesList"></param>
        private static void CalculateEMAForAllNotEMACandles(string connectionString, string scale, string calcType, List<string> durationsList, List<ShareObject> sharesList)
        {
            //Первичный расчет
            //Получаем набор свечей без расчета по EMA
            foreach (var share in sharesList)
            {
                log.Info("Ищу ЗАКРЫТЫЕ свечи без расчета по " + calcType + " для актива " + share.name + "(" + share.figi + ")");
                foreach (var dur in durationsList)
                {
                    log.Info("Обрабатываю длительность " + dur);
                    var candles = new PgExecuter(connectionString, log).GetCandlesWithoutCalculation(calcType, scale, share, dur);
                    candles = candles.OrderBy(o => o.candle_start_dt).ToList();

                    //Проверяем наличие предыдущего значения EMA для каждой свечи
                    foreach (var candle in candles)
                    {
                        log.Info("Для свечи candle_id = " + candle.id + " ищу предыдущее значение EMA");
                        float prevEMAValue = new PgExecuter(connectionString, log).GetPreviousValue(candle, dur, calcType);
                        if (prevEMAValue == -1)
                        {
                            log.Info("Для свечи candle_id = " + candle.id + " предыдущее значение EMA не найдено.");
                            log.Info("Для свечи candle_id = " + candle.id + " ищу предыдущее значение MA");
                            float prevMAValue = new PgExecuter(connectionString, log).GetPreviousValue(candle, dur, "MA");
                            if (prevMAValue == -1)
                            {
                                log.Info("Для свечи candle_id = " + candle.id + " не найдены предыдущие значения EMA и MA. Перехожу к следующей свече");
                            }
                            else
                            {
                                log.Info("Для свечи candle_id = " + candle.id + " найдено предыдуще значение MA = " + prevMAValue);

                                log.Info("Приступаю к расчету EMA для свечи candle_id = " + candle.id);

                                //рассчитываем EMA с применением MA (для первого прохода)
                                float alphaValue = new MathMACD().getAlphaValueForDuration(Convert.ToInt32(dur));
                                log.Info("Рассчитана alpha = " + alphaValue);
                                float curEMAValue = new MathMACD().getEMAValue(alphaValue, candle, prevMAValue);
                                log.Info("Рассчитана EMA = " + curEMAValue);
                                log.Info("Вставляю новую строку в calculations");
                                bool isInserted = new PgExecuter(connectionString, log).InsertIntoCalculationsTable(candle, calcType, Convert.ToInt32(dur), curEMAValue.ToString());
                                if (isInserted)
                                    log.Info("Успешно встравлено значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                                else
                                    log.Info("Не удалось вставить значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                            }
                        }
                        else
                        {
                            log.Info("Для свечи candle_id = " + candle.id + " найдено предыдуще значение EMA = " + prevEMAValue);

                            log.Info("Приступаю к расчету EMA для свечи candle_id = " + candle.id);

                            //рассчитываем EMA с применением MA (для первого прохода)
                            float alphaValue = new MathMACD().getAlphaValueForDuration(Convert.ToInt32(dur));
                            log.Info("Рассчитана alpha = " + alphaValue);
                            float curEMAValue = new MathMACD().getEMAValue(alphaValue, candle, prevEMAValue);
                            log.Info("Рассчитана EMA = " + curEMAValue);

                            bool isInserted = new PgExecuter(connectionString, log).InsertIntoCalculationsTable(candle, calcType, Convert.ToInt32(dur), curEMAValue.ToString());
                            if (isInserted)
                                log.Info("Успешно встравлено значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                            else
                                log.Info("Не удалось вставить значение EMA для candle_id = " + candle.id + " и длительности " + dur);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Логика рассчета MA
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scale"></param>
        /// <param name="appConfiguration"></param>
        /// <param name="calcType"></param>
        /// <param name="durationsList"></param>
        private static void MACalc(string connectionString, string scale, string appConfiguration, string calcType, List<string> durationsList)
        {
            //Расчет MA
            //Получаем список инструментов, по которым требуется произвести расчеты.
            var sharesList = new PgExecuter(connectionString, log).GetActualSharesList();
            log.Info("Список инструментов для расчета содержит " + sharesList.Count + " позиций");

            if (appConfiguration == "HistoryCalculate")
            {
                CalculateMAForAllNotMACAndles(connectionString, scale, durationsList, calcType, sharesList);
            }
            else if (appConfiguration == "RegularCalculate")
            {
                CalculateMAForOpenCandles(connectionString, scale, durationsList, calcType, sharesList);
            }
        }

        /// <summary>
        /// Рассчет MA для массива открытых свечей
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scale"></param>
        /// <param name="durationsList"></param>
        /// <param name="calcType"></param>
        /// <param name="sharesList"></param>
        private static void CalculateMAForOpenCandles(string connectionString, string scale, List<string> durationsList, string calcType, List<ShareObject> sharesList)
        {
            foreach (var share in sharesList)
            {
                log.Info("Ищу ОТКРЫТЫЕ свечи без расчета по " + calcType + " для актива " + share.name + "(" + share.figi + ")");

                foreach (var dur in durationsList)
                {
                    log.Info("\tПроверяю длительность: " + dur);
                    var candles = new PgExecuter(connectionString, log).GetOpenCandles(scale, share);
                    log.Info("\t\tНайдено " + candles.Count + " свечей");

                    log.Info("Расчитываю MA по активу " + share.name + "(" + share.figi + "), длительность интервала расчета = " + dur);
                    foreach (var candle in candles)
                    {
                        new MathMA(connectionString, log).MACalcOpenCandles(candle, dur, calcType);
                    }
                }
            }
        }

        /// <summary>
        /// Рассчет MA для массива закрытых свечей
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="scale"></param>
        /// <param name="durationsList"></param>
        /// <param name="calcType"></param>
        /// <param name="sharesList"></param>
        private static void CalculateMAForAllNotMACAndles(string connectionString, string scale, List<string> durationsList, string calcType, List<ShareObject> sharesList)
        {
            //Для каждого инструмента и длительности, а также масштабу находим все свечи, по которым НЕ рассчитана метрика MA 
            //Только закрытые свечи
            foreach (var share in sharesList)
            {

                //Первичный расчет
                log.Info("Ищу ЗАКРЫТЫЕ свечи без расчета по " + calcType + " для актива " + share.name + "(" + share.figi + ")");


                foreach (var dur in durationsList)
                {
                    log.Info("\tПроверяю длительность: " + dur);

                    var candles = new PgExecuter(connectionString, log).GetCandlesWithoutCalculation(calcType, scale, share, dur);
                    log.Info("\t\tНайдено " + candles.Count + " свечей");
                    candles = candles.OrderBy(o => o.candle_start_dt).ToList();

                    //Производим расчет MA
                    log.Info("Расчитываю MA по активу " + share.name + "(" + share.figi + "), длительность интервала расчета = " + dur);
                    foreach (var candle in candles)
                    {
                        new MathMA(connectionString, log).MACalcAll(candle, dur, calcType);
                    }
                }
            }
        }
    }
}