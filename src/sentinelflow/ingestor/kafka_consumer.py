import json
import requests
from confluent_kafka import Consumer, KafkaException, KafkaError
from loguru import logger
import sys
import os
import time

# Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
API_URL = os.getenv("API_URL", "http://localhost:8000/api/v1/transactions")
GROUP_ID = "sentinelflow-ingestor"

def create_consumer() -> Consumer:
    """Create and configure Kafka consumer."""
    config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    return Consumer(config)

def process_message(msg_value: bytes):
    """Process a single Kafka message and send to API."""
    try:
        # Parse JSON
        tx_data = json.loads(msg_value.decode('utf-8'))
        
        # Send to API
        # Ensure timestamp is string if it's not already
        if "timestamp" in tx_data and not isinstance(tx_data["timestamp"], str):
             tx_data["timestamp"] = str(tx_data["timestamp"])

        response = requests.post(API_URL, json=tx_data)
        
        if response.status_code == 200:
            result = response.json()
            status = "FRAUD" if result.get("is_fraud") else "CLEAN"
            logger.info(f"Processed: {tx_data.get('transaction_id')} | Status: {status}")
        else:
            logger.error(f"API Error {response.status_code}: {response.text}")
            
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode message: {e}")
    except requests.RequestException as e:
        logger.error(f"API Connection failed: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")

def main():
    logger.info(f"Starting Ingestor service... Broker: {KAFKA_BROKER}, Topic: {TOPIC}")
    
    # Wait for Kafka to be ready
    time.sleep(10) 
    
    consumer = create_consumer()
    consumer.subscribe([TOPIC])
    
    logger.info(f"Subscribed to {TOPIC}. Waiting for messages...")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
            
            process_message(msg.value())
            
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
