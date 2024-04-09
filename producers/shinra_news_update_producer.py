import json
import logging
from confluent_kafka import Producer
import requests
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_KEY = 'Bzfifjgrvc7rOkaE8ya9ytX9aGMnsoAjwmhAlEp6P5zISmv4'
NEWS_API_URL = f'https://api.currentsapi.services/v1/latest-news?apiKey={API_KEY}'

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_news():
    response = requests.get(NEWS_API_URL)
    if response.status_code == 200:
        news_items = response.json().get('news', [])
        return news_items
    else:
        logger.error(f"Failed to fetch news: HTTP {response.status_code}")
        return []

def produce_shinra_news(producer, topic):
    news_items = fetch_news()
    for item in news_items:
        # Transform the news item into a Shinra news event
        shinra_news_event = {
            "title": item['title'],
            "description": item['description'],
            "url": item['url'],
            "timestamp": item['published']
        }

        # Send the Shinra news event to Kafka
        producer.produce(topic, json.dumps(shinra_news_event).encode('utf-8'), callback=delivery_report)
        producer.poll(0)
    producer.flush()

if __name__ == "__main__":
    kafka_config = {'bootstrap.servers': 'localhost:9092'}
    topic = 'shinra-news-updates'
    producer = Producer(**kafka_config)
    produce_shinra_news(producer, topic)
