import os
import time
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import psycopg2
from psycopg2 import OperationalError as PostgresOperationalError

# Enhanced logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('log-consumer')

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'mysql_logs')
KAFKA_BROKERS = os.getenv('KAFKA_BROKER', '172.20.0.11:29092')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'mysqllogs')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', '172.20.0.14')

def wait_for_kafka(max_retries=30, retry_interval=10):
    """Wait for Kafka to become available."""
    logger.info(f"Waiting for Kafka broker at {KAFKA_BROKERS}...")
    
    for attempt in range(max_retries):
        try:
            # Test Kafka connection with a simple producer
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                api_version=(2, 8, 1),
                request_timeout_ms=10000,
                retry_backoff_ms=1000
            )
            producer.close()
            logger.info("Kafka broker is available!")
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Kafka not ready - {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
    
    logger.error(f"Kafka broker not available after {max_retries} attempts")
    return False

def connect_to_postgres(max_retries=30, retry_interval=5):
    """Connect to PostgreSQL with retry logic."""
    logger.info(f"Connecting to PostgreSQL at {POSTGRES_HOST}...")
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST,
                port=5432,
                connect_timeout=10
            )
            logger.info("Successfully connected to PostgreSQL.")
            return conn
        except PostgresOperationalError as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: PostgreSQL not ready - {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
        except Exception as e:
            logger.error(f"Unexpected error connecting to PostgreSQL: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
    
    raise Exception(f"Failed to connect to PostgreSQL after {max_retries} attempts")

def create_kafka_consumer():
    """Create Kafka consumer with proper configuration."""
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        group_id='log-consumer-group',
        api_version=(2, 8, 1),
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        max_poll_interval_ms=300000,
        request_timeout_ms=40000,
        retry_backoff_ms=1000,
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )

def setup_database(conn):
    """Setup database table."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id SERIAL PRIMARY KEY,
                    log_message TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            conn.commit()
            logger.info("Database table 'logs' is ready.")
    except Exception as e:
        logger.error(f"Failed to setup database: {e}")
        raise

def process_messages(consumer, db_conn):
    """Process messages from Kafka and store in PostgreSQL."""
    logger.info("Starting to consume messages...")
    
    while True:
        try:
            # Poll for messages with timeout
            messages = consumer.poll(timeout_ms=10000)
            
            if not messages:
                logger.debug("No messages received in the last 10 seconds")
                continue
            
            for topic_partition, message_batch in messages.items():
                for message in message_batch:
                    try:
                        log_entry = message.value
                        logger.info(f"Received log: {log_entry.strip()}")
                        
                        # Insert into PostgreSQL
                        with db_conn.cursor() as cur:
                            cur.execute(
                                "INSERT INTO logs (log_message) VALUES (%s);", 
                                (log_entry,)
                            )
                            db_conn.commit()
                            logger.info("Log successfully stored in database.")
                            
                    except Exception as e:
                        logger.error(f"Failed to process message: {e}")
                        db_conn.rollback()
                        
            # Commit offsets after processing batch
            consumer.commit_async()
            
        except KeyboardInterrupt:
            logger.info("Shutdown signal received.")
            break
        except Exception as e:
            logger.error(f"Error in message processing loop: {e}")
            time.sleep(5)  # Wait before retrying

def main():
    """Main function with proper error handling and recovery."""
    consumer = None
    db_conn = None
    
    try:
        # Wait for dependencies
        if not wait_for_kafka():
            raise Exception("Kafka broker not available")
        
        # Connect to databases
        db_conn = connect_to_postgres()
        setup_database(db_conn)
        
        # Create consumer
        consumer = create_kafka_consumer()
        logger.info(f"Subscribed to topic: {KAFKA_TOPIC}")
        
        # Start processing messages
        process_messages(consumer, db_conn)
        
    except KeyboardInterrupt:
        logger.info("Graceful shutdown initiated.")
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
    finally:
        # Cleanup resources
        try:
            if consumer:
                consumer.close()
                logger.info("Kafka consumer closed.")
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")
        
        try:
            if db_conn:
                db_conn.close()
                logger.info("Database connection closed.")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
        
        logger.info("Log consumer shutdown complete.")

if __name__ == "__main__":
    main()
