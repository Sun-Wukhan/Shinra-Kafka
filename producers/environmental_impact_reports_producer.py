import json
from confluent_kafka import Producer
import logging
import random
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_environmental_report(region):
    """Generate a simulated environmental impact report."""
    report = {
        "region": region,
        "timestamp": int(time.time()),
        "aqi": random.randint(0, 500),  # Air Quality Index
        "water_quality_score": random.randint(50, 100),
        "environmental_health_score": random.randint(0, 100)
    }
    return report

def produce_reports(producer, topic):
    regions = ['Midgar', 'Junon', 'Costa del Sol', 'Wutai']
    while True:
        for region in regions:
            report = generate_environmental_report(region)
            producer.produce(topic, json.dumps(report).encode('utf-8'), callback=delivery_report)
            producer.poll(0)
        producer.flush()
        logger.info("All reports sent for this cycle. Waiting for one hour before sending more.")
        time.sleep(3600)  # Wait for 1 hour before the next cycle

if __name__ == "__main__":
    kafka_config = {'bootstrap.servers': 'localhost:9092'}
    topic = 'environmental-impact-reports'
    producer = Producer(**kafka_config)
    produce_reports(producer, topic)
