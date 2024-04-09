import json
from confluent_kafka import Producer
import logging
import random
import time

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_distribution_metrics():
    sectors = range(1, 9)  # Midgar has 8 sectors
    status_options = ['Operational', 'Under Maintenance', 'Overloaded']
    events = ['None', 'Power Surge', 'Maintenance', 'Outage']

    while True:
        for sector_id in sectors:
            metrics = {
                "sector_id": sector_id,
                "timestamp": int(time.time()),
                "supplied_energy": round(random.uniform(100, 500), 2),
                "consumed_energy": round(random.uniform(80, 450), 2),
                "demand_forecast": round(random.uniform(100, 500), 2),
                "grid_status": random.choice(status_options),
                "peak_load": round(random.uniform(100, 600), 2),
                "emergency_events": random.choice(events)
            }
            yield metrics
        time.sleep(3600)  # Generate new metrics every hour

def produce_metrics(bootstrap_servers, topic):
    kafka_config = {'bootstrap.servers': bootstrap_servers}
    producer = Producer(**kafka_config)

    for metrics in generate_distribution_metrics():
        producer.produce(topic, json.dumps(metrics).encode('utf-8'), callback=delivery_report)
        producer.poll(0)  # Non-blocking
    producer.flush()

if __name__ == "__main__":
    produce_metrics('localhost:9092', 'energy-distribution-grid')
