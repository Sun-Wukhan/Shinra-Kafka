from confluent_kafka.admin import AdminClient

def list_topics(bootstrap_servers):
    """List all topics in the Kafka cluster."""
    admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
    cluster_metadata = admin_client.list_topics(timeout=10)  # Timeout in seconds
    return list(cluster_metadata.topics.keys())

if __name__ == "__main__":
    bootstrap_servers = 'localhost:9092'
    print("Topics in the cluster:", list_topics(bootstrap_servers))
