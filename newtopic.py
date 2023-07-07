from kafka.admin import KafkaAdminClient, NewTopic

# Define Kafka bootstrap servers
bootstrap_servers = "localhost:29092"

# Create an instance of the KafkaAdminClient
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Define the topic name and its configuration
topic_name = "latest_events"
partitions = 1
replication_factor = 1
config = {"cleanup.policy": "delete", "retention.ms": "86400000"}  # Additional topic configuration (optional)

# Create a NewTopic object
new_topic = NewTopic(name=topic_name, num_partitions=partitions, replication_factor=replication_factor, config=config)

# Create the topic
admin_client.create_topics(new_topics=[new_topic], validate_only=False)
