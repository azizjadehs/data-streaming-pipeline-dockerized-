from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_replace, lower, trim, count, avg, desc, from_json
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, MapType, IntegerType, FloatType, BooleanType
from pyspark.sql import functions as F

spark = SparkSession.builder \
.appName("SteamReviewParser") \
.master("spark://spark:7077") \
.config("spark.jars", "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
                           "/opt/bitnami/spark/jars/kafka-clients-3.5.0.jar,"
                           "/opt/bitnami/spark/jars/commons-pool2-2.11.1.jar,"
                           "/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar") \
.getOrCreate()

review_schema = StructType([
    StructField("language", StringType()),
    StructField("review", StringType()),
    StructField("timestamp_created", StringType()),
    StructField("voted_up", BooleanType()),
    StructField("votes_up", IntegerType()),
    StructField("author", StructType([
        StructField("steamid", StringType())
    ]))
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "steam_reviews") \
    .option("startingOffsets", "latest") \
    .load()
df_json = df_raw.selectExpr("CAST(value AS STRING) AS json_str") \
    .withColumn("data", from_json(col("json_str"), review_schema)) \
    .select(
        col("data.language").alias("language"),
        col("data.review").alias("review"),
        col("data.timestamp_created").alias("timestamp_created"),
        col("data.voted_up").alias("voted_up"),
        col("data.author.steamid").alias("steamid")
    )

query = df_json.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()
query.awaitTermination()
