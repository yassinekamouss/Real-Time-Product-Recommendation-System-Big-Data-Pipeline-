import os
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
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9094')
TOPIC_NAME = 'user-ratings'

# Determine the absolute path to Reviews.csv
# Assuming this script is in src/producer/ and the data is in data/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CSV_FILE_PATH = os.path.join(BASE_DIR, 'data', 'Reviews.csv')
CHUNK_SIZE = 1000

def create_producer():
    """Create and return a KafkaProducer instance with retry logic."""
    producer = None
    retries = 10
    retry_delay = 5
    
    while retries > 0:
        try:
            logger.info(f"Attempting to connect to Kafka broker at {KAFKA_BROKER}...")
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
        return

    try:
        # Read the CSV file in chunks to optimize memory usage
        for chunk in pd.read_csv(CSV_FILE_PATH, chunksize=CHUNK_SIZE):
            required_cols = ['UserId', 'ProductId', 'Score', 'Time']
            
            # Verify columns exist
            if not all(col in chunk.columns for col in required_cols):
                 logger.error(f"Missing required columns. Expected {required_cols}. Found: {list(chunk.columns)}")
                 break

            for index, row in chunk.iterrows():
                try:
                    # Extract and transform data into JSON format
                    message = {
                        'UserId': str(row['UserId']),
                        'ProductId': str(row['ProductId']),
                        'Score': float(row['Score']),
                        'Time': int(row['Time'])
                    }

                    # Send message to Kafka
                    producer.send(TOPIC_NAME, value=message)
                    
                    # Log entry creation at DEBUG level (avoid flooding INFO)
                    logger.debug(f"Sent: {message}")

                    # Simulate real-time stream with a random delay (0.1s to 0.5s)
                    time.sleep(random.uniform(0.1, 0.5))

                except Exception as e:
                    logger.error(f"Error processing row {index}: {e}")
                    
            logger.info(f"Successfully processed a chunk of {len(chunk)} records.")
            
    except Exception as e:
        logger.error(f"An unexpected error occurred during file processing: {e}")
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
    except Exception as e:
        logger.error(f"Fatal error in producer script: {e}")
