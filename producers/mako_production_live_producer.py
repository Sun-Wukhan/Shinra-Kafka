from confluent_kafka import Producer
import json
import logging
import random
import time

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def create_producer(config):
    """Create and configure a Kafka Producer."""
    producer = Producer(config)
    return producer

def simulate_mako_energy_data(producer, topic):
    """Generates and sends simulated Mako energy production data every hour."""
    reactor_ids = range(1, 5)  # Example reactor IDs
    status_options = ['Operational', 'Maintenance', 'Critical', 'Shutdown']

    while True:  # Continuous loop to generate data every hour
        for reactor_id in reactor_ids:
            status = random.choice(status_options)  # Choose a status each iteration
            message = {
                "reactor_id": reactor_id,
                "timestamp": int(time.time()),
                "energy_output": round(random.uniform(1000, 2000), 2),
                "status": status,
                "temperature": round(random.uniform(300, 500), 2),
                "pressure": round(random.uniform(2, 10), 2),
                "efficiency": round(random.uniform(70, 100), 2),
                "message": f"Reactor {reactor_id} is currently {status}."
            }

            # Asynchronously produce a message
            producer.produce(topic, json.dumps(message).encode('utf-8'), callback=delivery_report)
            
            # Poll the producer for events. Trigger callbacks
            producer.poll(0)
        
        # Wait for all messages to be delivered before generating more
        producer.flush()
        
        logger.info("Batch of messages sent. Waiting for one hour before sending more.")
        time.sleep(3600)  # Sleep for one hour (3600 seconds)

if __name__ == "__main__":
    kafka_config = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'mako-energy-producer'
    }

    topic = 'mako-production-live'
    
    producer = create_producer(kafka_config)
    try:
        simulate_mako_energy_data(producer, topic)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        producer.flush()
