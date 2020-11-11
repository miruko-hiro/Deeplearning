from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt
from pyspark.ml.feature import VectorAssembler
import numpy as np
from pyspark.ml.clustering import BisectingKMeans
import pyspark as ps
import findspark
findspark.init("C:\Spark")

spark = ps.sql.SparkSession.builder \
    .master("local") \
    .appName("Dimas Spark") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


#оч полезная статья по kmeans на spark
#https://rsandstroem.github.io/sparkkmeans.html

def data_preparation(dataset):
    # удалим из датафрейма все путые значение
    dataset = dataset.drop('_c9') #удаляет полностю столбец состоящий из Nan
    dataset = dataset.dropna() # удаляет строчки где есть Nan
    dataset.show()
    #конвертируем данные в float
    FEATURES_COL = dataset.columns
    FEATURES_COL.pop(0)
    # FEATURES_COL = ['_c1','_c10']
    for col in dataset.columns:
        if col in FEATURES_COL:
            dataset = dataset.withColumn(col, dataset[col].cast('float'))

    # в отличии от sklearn We need to store all features as an array of floats, and store this array as a column called "features"
    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    df_kmeans = vecAssembler.transform(dataset).select('_c0', 'features')

    return df_kmeans

# n - numbers of clusters
def search_opt_k(df_kmeans):
    # Trains a k-means model.
    # df_kmeans.show()
    #найдем оптимальное k методом локтя
    cost = np.zeros(20)
    for k in range(2, 20):
        kmeans = BisectingKMeans().setK(k).setSeed(1).setFeaturesCol("features")
        print('kmeans ', kmeans)
        model = kmeans.fit(df_kmeans.sample(False, 0.1, seed=42))
        cost[k] = model.computeCost(df_kmeans)  # requires Spark 2.0 or later
    # print(cost)
    # вмзуализируем локоть
    fig, ax = plt.subplots(1, 1, figsize=(8, 6))
    ax.plot(range(2, 20), cost[2:20])
    ax.set_xlabel('k')
    ax.set_ylabel('cost')
    plt.show()

# кластеризация по заданному кол-ву кластеров
def clustering(df_kmeans, n):
    kmeans = BisectingKMeans().setK(n).setSeed(1).setFeaturesCol("features")
    print('kmeans ', kmeans)
    model = kmeans.fit(df_kmeans)

    centers = model.clusterCenters()

    print("Cluster Centers: ")
    for center in centers:
        print(center)

# загружаем метрки из csv
df = spark.read.csv("metrics.csv")
df.show()
# подготовка датафрейма для кластеризации
df_for_kmeans = data_preparation(df)
# поиск оптимального k методом локтя
search_opt_k(df_for_kmeans)
# после поиска k построение кластеризации и вывод центров
clustering(df_for_kmeans, 8)
