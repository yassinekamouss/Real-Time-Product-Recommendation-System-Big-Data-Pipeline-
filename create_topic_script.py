from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(bootstrap_servers='kafka:29092')
topic = NewTopic(name='user-ratings', num_partitions=1, replication_factor=1)
try:
    admin.create_topics([topic])
    print("Topic created")
except Exception as e:
    print("Error or topic exists:", e)
