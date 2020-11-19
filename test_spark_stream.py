from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#Spark context details
sc = SparkContext(appName="Dimas Streaming Kafka")
ssc = StreamingContext(sc, 10)
#Creating Kafka direct stream
dks = KafkaUtils.createDirectStream(ssc, ["metrics"], {"metadata.broker.list": "localhost:9092"})
counts = dks.pprint()
#Starting Spark context
ssc.start()
ssc.awaitTermination()