from confluent_kafka import Consumer, KafkaError
import json
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def start_consumer(config, topics):
    """Start the Kafka consumer and process messages."""
    consumer = Consumer(config)
    consumer.subscribe(topics)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue  # No message available within timeout period

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.info('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                    continue
                else:
                    logger.error(msg.error())
                    break

            # Message is successfully received
            data = json.loads(msg.value().decode('utf-8'))
            process_message(data)
            
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()

def process_message(data):
    """Process and log the message received."""
    try:
        logger.info(f"Processing data from Reactor {data.get('reactor_id', 'Unknown')}:")
        logger.info(f"  Timestamp: {data.get('timestamp', 'Missing')}")
        logger.info(f"  Energy Output: {data.get('energy_output', 'Missing')} MWh")
        logger.info(f"  Status: {data.get('status', 'Missing')}")
        logger.info(f"  Temperature: {data.get('temperature', 'Missing')} K")
        logger.info(f"  Pressure: {data.get('pressure', 'Missing')} bar")
        logger.info(f"  Efficiency: {data.get('efficiency', 'Missing')}%")
        logger.info(f"  Message: {data.get('message', 'No message provided')}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


if __name__ == "__main__":
    # Consumer configuration
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mako-energy-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    topics = ['mako-production-live']

    start_consumer(consumer_config, topics)
