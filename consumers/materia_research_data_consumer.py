from confluent_kafka import Consumer, KafkaError
import logging
import json
import sys
import os

# Add the directory where 'common' is located to sys.path
script_path = os.path.abspath(__file__)  # Absolute path to this script
project_root = os.path.dirname(os.path.dirname(script_path))  # Project's root directory
sys.path.append(project_root)

from common.load_config import load_kafka_config

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def consume_messages(config_file):
    config = load_kafka_config(config_file)
    consumer = Consumer({
        'bootstrap.servers': config['bootstrap_servers'],
        'group.id': config['group_id'],
        'auto.offset.reset': config['auto_offset_reset']
    })
    consumer.subscribe(config['topics'])

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.info('End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
                else:
                    logger.error('Error: {}'.format(msg.error()))
                continue

            # Deserialize message
            materia_data = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Received Materia research data: {materia_data}")

    except KeyboardInterrupt:
        logger.info("Consumer operation stopped by user.")
    finally:
        logger.info("Closing consumer.")
        consumer.close()

if __name__ == "__main__":
    consume_messages('config/materia_research_data_consumer.json')
