# consumer/main.py

import os
import time
import logging
from kafka import KafkaConsumer
import psycopg2

# Basic logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get config from environment variables for flexibility
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'mysql_logs')
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'mysqllogs')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')

def connect_to_postgres():
    """Connects to the PostgreSQL database. Retries on failure."""
    conn = None
    while conn is None:
        try:
            conn = psycopg2.connect(
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST
            )
            logging.info("Successfully connected to PostgreSQL.")
        except psycopg2.OperationalError as e:
            logging.error(f"Could not connect to PostgreSQL: {e}. Retrying in 5 seconds...")
            time.sleep(5)
    return conn

def setup_database(conn):
    """Ensures the logs table exists."""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                log_message TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        conn.commit()
        logging.info("Database table 'logs' is ready.")

def main():
    """Main function to consume logs and store them."""
    # Connecting to postgres
    db_conn = connect_to_postgres()
    setup_database(db_conn)

    # Setup Kafka consumer with retry logic
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset='earliest', # Start reading from the beginning of the topic
                group_id='log-consumer-group'
            )
            logging.info("Successfully connected to Kafka.")
        except Exception as e:
            logging.error(f"Could not connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

    # Main loop to process messages
    try:
        for message in consumer:
            log_entry = message.value.decode('utf-8')
            logging.info(f"Received log: {log_entry.strip()}")

            try:
                # Insert the log into PostgreSQL
                with db_conn.cursor() as cur:
                    cur.execute("INSERT INTO logs (log_message) VALUES (%s);", (log_entry,))
                    db_conn.commit()
            except Exception as e:
                logging.error(f"Failed to insert log into DB: {e}")
                # In a real app, you might want to handle this better
                # e.g., send to a dead-letter queue or retry
    except KeyboardInterrupt:
        logging.info("Shutting down consumer.")
    finally:
        if db_conn:
            db_conn.close()
        if consumer:
            consumer.close()

if __name__ == "__main__":
    main()