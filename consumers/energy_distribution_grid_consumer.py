from confluent_kafka import Consumer, KafkaError
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def start_consumer(bootstrap_servers, group_id, topics):
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(topics)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(msg.error())
                    break

            metrics = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Received metrics: {metrics}")

    except KeyboardInterrupt:
        logger.info("Consumer operation stopped by user.")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_consumer('localhost:9092', 'energy-distribution-group', ['energy-distribution-grid'])
