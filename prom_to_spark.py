import prometheus_api_client as pac
import prometheus_api_client.utils as pac_u
import pyspark as ps
import findspark
findspark.init("C:\Spark")

spark = ps.sql.SparkSession.builder \
    .master("local") \
    .appName("Dimas Spark") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

def dataframe_creation():
    # точка входа в программирование Spark с помощью Dataset и DataFrame API.
    # master - задает главный URL-адрес Spark для подключения, например «local» для локального запуска
    # appName - устанавливает имя приложения, которое будет отображаться в веб-интерфейсе Spark
    # config - устанавливает параметр конфигурации
    # getOrCreate() - получает существующий SparkSession или, если его нет, создает новый на основе параметров, установленных в этом построителе

    # конект к прометеусу
    prom = pac.PrometheusConnect(url="http://localhost:9090/", disable_ssl=True)

    # получение названия всех метрик прометеуса
    all_metrics = prom.all_metrics()
    metrics = list()

    # установка рейнджа времени для данных
    # в данном случае от "минус 1 день" до "сейчас"
    start_time = pac_u.parse_datetime("1d")
    end_time = pac_u.parse_datetime("now")

    dataframe_list = dict()

    # цикл, позволяющий выбрать только те метрики, которые нам нужны,
    # а именно в которых есть название application и process.
    # такими словами помечены собираемые метрики с приложения.

    """
    for i in all_metrics:
        if i.split('application')[0] == '' or i.split('process')[0] == '':
            metric_data = prom.get_metric_range_data(
                i,
                start_time=start_time,
                end_time=end_time
            )
            if metric_data:
                # берем только значения метрики
                values = metric_data[0]['values']
                # создаем датафрейм со значениями метрики и сохраняем его в словаре,
                # где ключ - название метрики
                print(values)
                dataframe_list[i] = (spark.createDataFrame(values, ['time', 'value'])
    """
    dataframe = None
    for i in all_metrics:
        if i.split('application')[0] == '': #or i.split('process')[0] == '':
            metric_data = prom.get_metric_range_data(
                i,
                start_time=start_time,
                end_time=end_time
            )
            if metric_data:
                # берем только значения метрики
                values = metric_data[0]['values']
                if dataframe is None:
                    dataframe = spark.createDataFrame(values, ['time', i])
                else:
                    df = spark.createDataFrame(values, ['time', i])
                    dataframe = dataframe.join(df, 'time', 'right')

    # dataframe.show()
    dataframe.write.csv('metrics.csv')
    return dataframe

dataframe_creation()