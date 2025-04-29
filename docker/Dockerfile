FROM bitnami/spark:latest

USER root

# Optional: only if you need Python dependencies
RUN pip install --no-cache-dir kafka-python py4j
# Install Python and pip
RUN apt-get update && apt-get install -y curl
RUN pip install kafka-python


# Create JARs directory
RUN mkdir -p /opt/bitnami/spark/jars

# Download required Kafka JARs
RUN curl -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/kafka-clients-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    curl -o /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -o /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# Set correct permissions
RUN chmod -R 777 /opt/bitnami/spark/jars

USER 1001



#CMD ["bash", "-c", "sleep 5 && /opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.master.Master"]
#CMD ["bash", "-c", "sleep 5 && /opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077"]