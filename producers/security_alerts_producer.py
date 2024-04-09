import json
from confluent_kafka import Producer
import logging
import time

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def read_security_alerts(file_path):
    """Read security alerts from a JSON file."""
    with open(file_path, 'r') as file:
        alerts = json.load(file)
        return alerts

def produce_security_alerts(producer, topic, file_path):
    """Produce security alerts to Kafka topic from a JSON file every hour."""
    while True:  # Continuously produce alerts every hour
        alerts = read_security_alerts(file_path)
        for alert in alerts:
            producer.produce(topic, json.dumps(alert).encode('utf-8'), callback=delivery_report)
            producer.poll(0)
        
        producer.flush()
        logger.info("All alerts sent for this hour. Waiting for one hour before sending more.")
        time.sleep(3600)  # Wait for 1 hour (3600 seconds) before sending the next batch

if __name__ == "__main__":
    kafka_config = {
        'bootstrap.servers': 'localhost:9092'
    }
    topic = 'security-alerts'
    file_path = './assets/security_alerts.json'  # Ensure this path points to your JSON file

    producer = Producer(**kafka_config)
    try:
        produce_security_alerts(producer, topic, file_path)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        producer.flush()
