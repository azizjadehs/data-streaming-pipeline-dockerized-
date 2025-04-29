from kafka import KafkaProducer
import time

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers='kafka:9092')

# Send test messages
producer.send('test-topic', b'Hello Kafka!')
producer.flush()  # ✅ Ensure the message is actually sent

# Print confirmation
print("✅ Message sent to Kafka!")

# Wait a bit before closing to avoid timeout
time.sleep(2)  # ✅ Small delay to let Kafka process before closing

# Close the producer properly
producer.close()
print("✅ Kafka")