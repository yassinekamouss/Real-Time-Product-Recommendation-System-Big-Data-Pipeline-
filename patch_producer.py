import re

with open('src/producer/kafka_producer.py', 'r') as f:
    content = f.read()

new_create_producer = """def create_producer():
    \"\"\"Create and return a KafkaProducer instance with retry logic and robust buffering.\"\"\"
    producer = None
    retries = 10
    retry_delay = 5
    
    while retries > 0:
        try:
            logger.info(f"Attempting to connect to Kafka broker at {KAFKA_BROKER}...")
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',  # Plus robuste que 1 : attend l'acquittement de tous les replicas
                retries=5,   # NOUVEAU: Retentatives d'envoi internes au producer Kafka
                buffer_memory=33554432, # NOUVEAU: 32MB de buffer
                batch_size=16384,
                linger_ms=10, # Légèrement augmenté pour favoriser les batchs
                request_timeout_ms=30000,
                max_block_ms=60000 # NOUVEAU: Timeout global pour le blocage (ex: buffer plein)
            )
            logger.info("Successfully connected to Kafka Broker.")
            break
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker not available. Retrying in {retry_delay} seconds... ({retries} retries left)")
            retries -= 1
            time.sleep(retry_delay)
    
    if not producer:
        logger.error("Failed to connect to Kafka broker after multiple retries. Exiting.")
        raise Exception("Kafka broker unavailable")
        
    return producer
"""

content = re.sub(r'def create_producer\(\):.*?return producer', new_create_producer, content, flags=re.DOTALL)

with open('src/producer/kafka_producer.py', 'w') as f:
    f.write(content)

