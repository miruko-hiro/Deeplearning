from prom_to_spark import dataframe_creation
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt
from pyspark.ml.feature import VectorAssembler
import numpy as np
from pyspark.ml.clustering import BisectingKMeans

#оч полезная статья по kmeans на spark
#https://rsandstroem.github.io/sparkkmeans.html

# n - numbers of clusters
def clustering(dataset):
    # Trains a k-means model.
    #конвертируем данные в float
    # FEATURES_COL = dataset.columns
    # FEATURES_COL.pop(0)
    FEATURES_COL = ['application_http_status_total','application_httprequests_active']
    for col in dataset.columns:
        if col in FEATURES_COL:
            dataset = dataset.withColumn(col, dataset[col].cast('float'))

    # удалим из датафрейма все путые значение
    dataset = dataset.dropna()

    # в отличии от sklearn We need to store all features as an array of floats, and store this array as a column called "features"
    vecAssembler = VectorAssembler(inputCols=FEATURES_COL, outputCol="features")
    df_kmeans = vecAssembler.transform(dataset).select('time', 'features')
    df_kmeans.show()
    #найдем оптимальное k методом локтя
    cost = np.zeros(20)
    for k in range(2, 20):
        kmeans = BisectingKMeans().setK(k).setSeed(1).setFeaturesCol("features")
        model = kmeans.fit(df_kmeans.sample(False, 0.1, seed=42))
        cost[k] = model.computeCost(df_kmeans)  # requires Spark 2.0 or later
    print(cost)
    # вмзуализируем локоть
    fig, ax = plt.subplots(1, 1, figsize=(8, 6))
    ax.plot(range(2, 20), cost[2:20])
    ax.set_xlabel('k')
    ax.set_ylabel('cost')
    plt.show()


#создаем датафрейм с метриками
df = dataframe_creation()

# кластеризация данных по метрикам
clustering(df)

# score = []
# for i in range(10):
#     tmp = clustering(df, i)
#     score.append(tmp)
#
# plt.plot(range(1, 8), score, marker='s')
# plt.show()
