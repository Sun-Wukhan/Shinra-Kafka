from confluent_kafka import Consumer, KafkaError
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def start_consumer(config, topics):
    consumer = Consumer(config)
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

            report = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Received environmental impact report: {report}")

    except KeyboardInterrupt:
        logger.info("Consumer operation stopped by user.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'environmental-impact-group',
        'auto.offset.reset': 'earliest'
    }

    topics = ['environmental-impact-reports']
    start_consumer(consumer_config, topics)
