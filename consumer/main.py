import os
import time
import logging
from kafka import KafkaConsumer
import psycopg2

# Basic logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'mysql_logs')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', '172.20.0.11:29092')  
POSTGRES_DB = os.getenv('POSTGRES_DB', 'mysqllogs')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', '172.20.0.14')  

def connect_to_postgres(max_retries=30, retry_interval=5):
    """Connects to the PostgreSQL database with better retry logic."""
    conn = None
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST,
                connect_timeout=10
            )
            logging.info("Successfully connected to PostgreSQL.")
            return conn
        except PostgresOperationalError as e:
            logging.error(f"Attempt {attempt + 1}/{max_retries}: Could not connect to PostgreSQL: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
    
    raise Exception(f"Failed to connect to PostgreSQL after {max_retries} attempts")

def create_kafka_consumer(max_retries=30, retry_interval=5):
    """Creates Kafka consumer with better retry logic."""
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest',
                group_id='log-consumer-group',
                api_version=(2, 8, 1),  
                consumer_timeout_ms=10000
            )
            logging.info("Successfully connected to Kafka.")
            return consumer
        except NoBrokersAvailable as e:
            logging.error(f"Attempt {attempt + 1}/{max_retries}: Kafka brokers not available: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
        except Exception as e:
            logging.error(f"Attempt {attempt + 1}/{max_retries}: Could not connect to Kafka: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
    
    raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")

def main():
    """Main function to consume logs and store them."""
    try:
        # Connecting to postgres
        db_conn = connect_to_postgres()
        
        # Setup database
        with db_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id SERIAL PRIMARY KEY,
                    log_message TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                );
            """)
            db_conn.commit()
            logging.info("Database table 'logs' is ready.")

        # Create Kafka consumer
        consumer = create_kafka_consumer()

        # Main loop to process messages
        logging.info("Starting to consume messages...")
        for message in consumer:
            log_entry = message.value.decode('utf-8')
            logging.info(f"Received log: {log_entry.strip()}")

            try:
                # Insert the log into PostgreSQL
                with db_conn.cursor() as cur:
                    cur.execute("INSERT INTO logs (log_message) VALUES (%s);", (log_entry,))
                    db_conn.commit()
                    logging.info("Log successfully stored in database.")
            except Exception as e:
                logging.error(f"Failed to insert log into DB: {e}")
                db_conn.rollback()

    except KeyboardInterrupt:
        logging.info("Shutting down consumer.")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
    finally:
        if 'db_conn' in locals() and db_conn:
            db_conn.close()
        if 'consumer' in locals() and consumer:
            consumer.close()

if __name__ == "__main__":
    main()
