import os
import sys
import json
import time
import random
import logging
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('KafkaProducer')

# Configuration
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:29092')
TOPIC_NAME = 'user-ratings'

# Chemin absolu mis à jour pour correspondre au nouveau volume partagé Airflow/Spark
CSV_FILE_PATH = '/opt/spark/data/Reviews.csv'
CHUNK_SIZE = 50000

def create_producer():
    """Create and return a KafkaProducer instance with retry logic and robust buffering."""
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


def process_and_send_data(producer):
    """Read CSV in chunks and send to Kafka topic with random delays."""
    logger.info(f"Starting to process file: {CSV_FILE_PATH}")
    
    if not os.path.exists(CSV_FILE_PATH):
        logger.error(f"File not found: {CSV_FILE_PATH}. Please ensure the dataset is in the correct location.")
        sys.exit(1)

    try:
        # Read the CSV file in chunks to optimize memory usage
        for chunk in pd.read_csv(CSV_FILE_PATH, chunksize=CHUNK_SIZE):
            required_cols = ['Id', 'UserId', 'ProductId', 'Score', 'Time']
            
            # Verify columns exist
            if not all(col in chunk.columns for col in required_cols):
                 logger.error(f"Missing required columns. Expected {required_cols}. Found: {list(chunk.columns)}")
                 sys.exit(1)

            for index, row in chunk.iterrows():
                try:
                    # Split logique deterministe pour eviter le data leakage (streaming 40%).
                    if int(row['Id']) % 10 >= 6:
                        message = {
                            'UserId': str(row['UserId']),
                            'ProductId': str(row['ProductId']),
                            'Score': float(row['Score']),
                            'Time': int(row['Time'])
                        }

                        producer.send(TOPIC_NAME, value=message)
                    
                    #time.sleep(random.uniform(0.001, 0.01))

                except Exception as e:
                    logger.error(f"Error processing row {index}: {e}")
                    
            logger.info(f"Successfully processed a chunk of {len(chunk)} records.")
        # --- À la fin de ta boucle d'envoi ---
        logger.info("Fin de la lecture du CSV. Envoi des messages restants (Flush)...")

        producer.flush(timeout=30) 

        logger.info("Kafka producer proprement fermé.")

    except FileNotFoundError as e:
        logger.error(f"Erreur fatale: Le fichier CSV n'a pas été trouvé. {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred during file processing: {e}")
        sys.exit(1)
    finally:
        if producer:
            logger.info("Flushing messages and closing producer...")
            producer.flush()
            producer.close()
            logger.info("Kafka producer gracefully closed.")

if __name__ == '__main__':
    try:
        kafka_producer = create_producer()
        process_and_send_data(kafka_producer)
        logger.info("Ingestion terminée avec succès.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error in producer script: {e}")
        sys.exit(1)
