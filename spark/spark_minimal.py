from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaSpark") \
    .master("spark://spark:7077") \
    .config("spark.jars", "/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
                           "/opt/bitnami/spark/jars/kafka-clients-3.5.0.jar,"
                           "/opt/bitnami/spark/jars/commons-pool2-2.8.0.jar,"
                           "/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar") \
    .getOrCreate()
