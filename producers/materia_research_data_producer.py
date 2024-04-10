import json
from confluent_kafka import Producer
import logging
import random
import time
from datetime import datetime
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

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def generate_materia_data():
    materia_types = ['Healing', 'Fire', 'Ice', 'Lightning', 'Wind', 'Earth', 'Poison', 'Barrier', 'Time', 'Space']
    locations = ['Midgar', 'Nibelheim', 'Cosmo Canyon', 'Wutai', 'Ancient Forest', 'Gongaga', 'Rocket Town']
    power_levels = range(1, 100)

    # Generate a dynamic discovery date
    discovery_date = datetime.now().strftime("%Y-%m-%d")

    materia_data = {
        "type": random.choice(materia_types),
        "power_level": random.choice(power_levels),
        "location_found": random.choice(locations),
        "discovery_date": discovery_date,
        "researcher": "Adina Khan"  
    }
    return materia_data

def produce_messages(config_file):
    config = load_kafka_config(config_file)
    producer = Producer({'bootstrap.servers': config['bootstrap_servers']})
    topic = config['topic']  # Ensure this line correctly retrieves the topic from your config

    while True:
        materia_data = generate_materia_data()
        
        try:
            logger.info(f"Sending materia data: {materia_data}")
            # Ensure the 'topic' variable is used here
            producer.produce(topic, json.dumps(materia_data).encode('utf-8'), callback=delivery_report)
            producer.poll(0)  # Serve delivery report callbacks from previous produce() calls
        except Exception as e:
            logger.error(f"Error producing message: {e}")
        
        producer.flush()
        logger.info("Waiting 5 minutes for the next materia discovery...")
        time.sleep(300)  # Wait for 5 minutes (300 seconds) before the next iteration


if __name__ == "__main__":
    produce_messages('config/materia_research_data_producer.json')
