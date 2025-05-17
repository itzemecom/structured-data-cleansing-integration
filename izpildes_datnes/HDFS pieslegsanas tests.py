from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HDFS tests") \
    .getOrCreate()

df = spark.read.text("hdfs://namenode:9000/test/test.txt")
df.show()