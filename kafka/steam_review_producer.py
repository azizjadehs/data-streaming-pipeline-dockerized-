import requests
import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

App_ID = 1604030 #app Id for V rising
KAFKA_TOPIC = 'steam_reviews'  # Kafka topic to send messages to
print("Starting Kafka producer...")
print("Connecting to Kafka...")

producer = KafkaProducer( 
    bootstrap_servers='kafka:9092',  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),  # Serialize JSON data
    retries=5,  # Number of retries for sending messages
    acks='all'  # Wait for all replicas to acknowledge the message
)
print("Kafka producer created!")

print("Connected to Kafka!")
# Function to fetch Steam reviews
def fetch_steam_reviews(app_id, count=100):
    url = f"https://store.steampowered.com/appreviews/{app_id}?json=1&count={count}"
    response = requests.get(url)
    print("Fetching Steam reviews...")
    if response.status_code == 200:
        return response.json().get('reviews', [])
    else:
        print(f"Error fetching reviews: {response.status_code}")
        return []
    
while True:
    # Fetch reviews
    reviews = fetch_steam_reviews(App_ID, count=100)
    
    # Send reviews to Kafka
    for review in reviews:
        try:
            producer.send(KAFKA_TOPIC, review)  # Send each review to Kafka topic
            print(f"Sent review to Kafka: {review}")  # Print confirmation for each sent review
        except KafkaError as e:
            print(f"Error sending message to Kafka: {e}")  # Print error if sending fails
    
    
    # Wait before fetching again
    producer.flush()  # Ensure all messages are sent before waiting
    time.sleep(30)  # Wait for 1 minute before fetching new reviews
