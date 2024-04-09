from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

def create_topics(admin_client, topics):
    """Create Kafka topics."""
    fs = admin_client.create_topics(topics)

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None if successful
            print(f"Successfully created topic: {topic}")
        except KafkaException as e:
            # Handle topic creation failure
            print(f"Failed to create topic {topic}: {e}")

def main():
    # Configuration for connecting to your Kafka cluster
    kafka_config = {
        'bootstrap.servers': 'localhost:9092'
    }

    # Initialize AdminClient with your Kafka configuration
    admin_client = AdminClient(kafka_config)

    # Define your topics and their configurations here
    topics = [
    NewTopic("mako-production-live", num_partitions=3, replication_factor=1),
    NewTopic("reactor-status-events", num_partitions=3, replication_factor=1),
    NewTopic("energy-distribution-grid", num_partitions=2, replication_factor=1),
    NewTopic("sector-energy-usage", num_partitions=2, replication_factor=1),
    NewTopic("customer-usage-details", num_partitions=3, replication_factor=1),
    NewTopic("customer-service-requests", num_partitions=2, replication_factor=1),
    NewTopic("environmental-impact-reports", num_partitions=2, replication_factor=1),
    NewTopic("lifestream-fluctuations", num_partitions=1, replication_factor=1),
    NewTopic("security-alerts", num_partitions=3, replication_factor=1),
    NewTopic("avalanche-activities", num_partitions=2, replication_factor=1),
    NewTopic("materia-research-data", num_partitions=2, replication_factor=1),
    NewTopic("weapon-development-logs", num_partitions=2, replication_factor=1),
    NewTopic("shinra-news-updates", num_partitions=1, replication_factor=1),
    NewTopic("executive-communications", num_partitions=1, replication_factor=1),
    NewTopic("mr-man", num_partitions=1, replication_factor=1),
]
    create_topics(admin_client, topics)

if __name__ == "__main__":
    main()
