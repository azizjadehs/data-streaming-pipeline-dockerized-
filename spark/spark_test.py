from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SimpleTest") \
    .master("spark://spark:7077") \
    .getOrCreate()

df = spark.range(1, 10)
df.show()
