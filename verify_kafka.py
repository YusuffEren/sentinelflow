from confluent_kafka import Producer, Consumer, KafkaError
import sys
import time
import json

CONF = {'bootstrap.servers': 'localhost:9092'}
TOPIC = 'test_topic'

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def verify_producer():
    print("Verifying Producer...")
    p = Producer(CONF)
    try:
        p.produce(TOPIC, json.dumps({"test": "value"}).encode('utf-8'), callback=delivery_report)
        p.flush()
        print("Producer check passed!")
        return True
    except Exception as e:
        print(f"Producer failed: {e}")
        return False

def verify_consumer():
    print("Verifying Consumer...")
    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest'
    })
    c.subscribe([TOPIC])
    
    try:
        msg = c.poll(5.0)
        if msg is None:
            print("Consumer timed out (no message)")
            return False
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            return False
            
        print(f"Received message: {msg.value().decode('utf-8')}")
        print("Consumer check passed!")
        return True
    except Exception as e:
        print(f"Consumer failed: {e}")
        return False
    finally:
        c.close()

if __name__ == "__main__":
    if verify_producer() and verify_consumer():
        print("\nKafka infrastructure is HEALTHY! ✅")
    else:
        print("\nKafka infrastructure FAILED! ❌")
        sys.exit(1)
