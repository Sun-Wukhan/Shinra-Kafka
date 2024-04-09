from confluent_kafka import Consumer, KafkaError
import json
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def start_consumer(config, topics):
    consumer = Consumer(config)
    consumer.subscribe(topics)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue  # No message available within timeout period

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.info(f'End of partition reached {msg.topic()}[{msg.partition()}]')
                else:
                    logger.error(f'Consumer error: {msg.error()}')
                    continue

            # Successfully received a message
            alert = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Received security alert: {alert}")

    except KeyboardInterrupt:
        logger.info("Consumer operation stopped by user.")
    finally:
        # Clean up on exit
        consumer.close()

if __name__ == "__main__":
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'security-alerts-group',
        'auto.offset.reset': 'earliest'
    }

    topics = ['security-alerts']

    start_consumer(consumer_config, topics)
