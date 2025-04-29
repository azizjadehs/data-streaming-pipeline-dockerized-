from pyspark.sql import SparkSession

#spark = SparkSession.builder \
 #   .appName("SparkKafkaIntegration") \
  #  .master("spark://spark:7077") \
   # .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/*") \
    #.config("spark.jars", "/opt/bitnami/spark/jars/*") \
    #.config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/*") \
    #.getOrCreate()

spark = SparkSession.builder \
    .appName("SparkKafkaIntegration") \
    .master("spark://spark:7077") \
    .config("spark.jars", "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
                           "/opt/bitnami/spark/jars/kafka-clients-3.5.0.jar,"
                           "/opt/bitnami/spark/jars/commons-pool2-2.11.1.jar,"
                           "/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar") \
    .getOrCreate()


'''
jar_paths = ",".join([
    "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "/opt/bitnami/spark/jars/kafka-clients-3.5.0.jar",
    "/opt/bitnami/spark/jars/commons-pool2-2.8.0.jar",
    "/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar"
])

spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .master("spark://spark:7077") \
    .config("spark.jars", jar_paths) \
    .config("spark.submit.pyFiles", "") \
    .getOrCreate()
``
print("✅ Spark is using JARs:", spark.sparkContext._conf.get("spark.jars"))
print("✅ Spark version:", spark.version)

'''

try:
    print("🔥 Connecting to Kafka...")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "test-topic") \
        .option("startingOffsets", "earliest") \
        .load()
    
    print("🔥 Kafka Stream Connected!")


    df = df.selectExpr("CAST(value AS STRING) AS data")
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    print("🔥 Waiting for messages...")

    query.awaitTermination()

except Exception as e:
    print(f"🔥 ERROR: {str(e)}")