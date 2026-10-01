import os
import json
import time
import uuid
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load environment variables from the .env file at the repo root
# find_dotenv() walks up the directory tree to locate the file automatically
from dotenv import find_dotenv
load_dotenv(find_dotenv())

# Fetch configurations with fallbacks
KAFKA_BROKER = os.getenv('KAFKA_BROKER_EXTERNAL')
TOPIC_NAME = os.getenv('TRANSACTIONS_TOPIC')

# Initialize the Confluent Kafka Producer
conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'finstream-mock-producer',
    'acks': 'all' 
}

producer = Producer(conf)

def delivery_report(err, msg):
    """
    Callback triggered by poll() or flush() upon message delivery 
    success or failure.
    """
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} "
              f"[Partition: {msg.partition()}] at Offset: {msg.offset()}")

def generate_transaction():
    """Generates a mock financial transaction payload."""
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"USER_{random.randint(1000, 9999)}",
        "merchant_id": f"MERCH_{random.randint(100, 999)}",
        "amount": round(random.uniform(5.0, 3000.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "lat": round(random.uniform(-90.0, 90.0), 6),
        "lon": round(random.uniform(-180.0, 180.0), 6),
        "is_flagged": False
    }

def main():
    print(f"Starting FinStream mock generator on {KAFKA_BROKER}")
    print(f"Target topic: {TOPIC_NAME}")
    print("Press Ctrl+C to stop...\n")
    
    try:
        while True:
            transaction = generate_transaction()
            
            # Confluent Kafka expects bytes, so we serialize the dict to JSON, then encode
            payload = json.dumps(transaction).encode('utf-8')
            
            # Use the user_id as the partition key to guarantee ordering per user
            key = transaction['user_id'].encode('utf-8')
            
            # produce() is asynchronous. It merely enqueues the message internally.
            producer.produce(
                topic=TOPIC_NAME,
                key=key,
                value=payload,
                callback=delivery_report
            )
            
            # poll() triggers the delivery_report callbacks for previously delivered messages
            producer.poll(0)
            
            # Simulate real-time streaming velocity (1 to 10 events per second)
            time.sleep(random.uniform(0.1, 1.0))
            
    except KeyboardInterrupt:
        print("\nStopping transaction generator...")
    finally:
        # flush() ensures all internal buffers are emptied and blocks until all 
        # in-flight messages are delivered before shutting down.
        print("Flushing outstanding messages to broker...")
        producer.flush()

if __name__ == '__main__':
    main()