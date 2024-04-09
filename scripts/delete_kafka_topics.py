from confluent_kafka.admin import AdminClient
from confluent_kafka import KafkaException
import argparse
import sys

def delete_topic(bootstrap_servers, topic_name):
    # Create AdminClient
    try:
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    except KafkaException as e:
        print(f"Failed to create AdminClient: {e}")
        sys.exit(1)

    # Check if the topic exists before attempting deletion
    try:
        cluster_metadata = admin_client.list_topics(timeout=10)
        if topic_name not in cluster_metadata.topics:
            print(f"Topic {topic_name} does not exist.")
            sys.exit(1)
    except KafkaException as e:
        print(f"Failed to list topics: {e}")
        sys.exit(1)

    # Delete the topic
    try:
        fs = admin_client.delete_topics([topic_name], operation_timeout=30)

        # Wait for operation to complete
        for topic, f in fs.items():
            try:
                f.result()  # The result itself is None if successful
                print(f"Topic {topic} deleted")
            except Exception as e:
                print(f"Failed to delete topic {topic}: {e}")
    except KafkaException as e:
        print(f"Failed to delete topic: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Setup CLI argument parsing
    parser = argparse.ArgumentParser(description="Delete a Kafka topic.")
    parser.add_argument("topic_name", help="The name of the topic to delete.")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka cluster bootstrap servers (default: localhost:9092)")
    
    args = parser.parse_args()

    # Pass the CLI arguments to the delete_topic function
    delete_topic(args.bootstrap_servers, args.topic_name)

# python scripts/delete_kafka_topics.py mr-man