import os
import logging
import sys
import confluent_kafka
from kafka.admin import KafkaAdminClient, NewTopic

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

# Environment configuration
kafka_brokers = os.getenv("REDPANDA_BROKERS")
topic_name = os.getenv("KAFKA_TOPIC")


def _require_env(var, name):
    if not var:
        logging.error(f"Environment variable {name} is not set. Please set it in .env or your environment.")
        raise SystemExit(1)


def create_topic(retries: int = 3):
    # Validate required env settings
    _require_env(kafka_brokers, 'REDPANDA_BROKERS')
    _require_env(topic_name, 'KAFKA_TOPIC')

    admin_client = KafkaAdminClient(bootstrap_servers=kafka_brokers, client_id='publish_data')

    # Ensure topic_name is a valid string
    if not isinstance(topic_name, str) or topic_name.strip() == "":
        logging.error("KAFKA_TOPIC must be a non-empty string")
        raise SystemExit(1)

    attempts = 0
    while attempts <= retries:
        try:
            logging.info(f"Checking for topic '{topic_name}' on {kafka_brokers} (attempt {attempts + 1})")
            metadata = admin_client.list_topics()
            # list_topics returns a ClusterMetadata; use .topics to check membership
            topics = getattr(metadata, 'topics', {})
            if topic_name not in topics:
                topic = NewTopic(name=topic_name, num_partitions=10, replication_factor=1)
                logging.info(f"Creating topic '{topic_name}'")
                admin_client.create_topics(new_topics=[topic], validate_only=False)
            else:
                logging.info(f"Topic '{topic_name}' already exists")
            return
        except Exception as e:  # kafka.errors.KafkaConnectionError or others
            attempts += 1
            logging.warning(f"Could not create/verify topic (attempt {attempts}/{retries}): {e}")
            if attempts > retries:
                logging.error("Exceeded retries while creating topic. Exiting.")
                raise
            # simple backoff
            import time
            time.sleep(1 + attempts)


def get_kafka_producer():
    logging.info(f"Connecting to kafka")
    config = {'bootstrap.servers': kafka_brokers}
    # Validate setting
    _require_env(kafka_brokers, 'REDPANDA_BROKERS')
    try:
        p = confluent_kafka.Producer(**config)
        # quick metadata check to surface connection issues early
        p.list_topics(timeout=5)
        return p
    except Exception as e:
        logging.error(f"Failed to create Kafka producer: {e}")
        raise


if __name__ == "__main__":  
    producer = get_kafka_producer()
    create_topic()
    for message in sys.stdin:
        if message != 'n':
            failed = True
            while failed:
                try:
                    # Strip trailing newline from message before produce
                    payload = message.rstrip('\n')
                    producer.produce(topic_name, value=payload.encode('utf-8'))
                    failed = False
                except BufferError as e:
                    producer.flush()
                
        else:
            break
    # ensure deliveries are completed
    try:
        producer.flush(timeout=10)
    except Exception:
        logging.warning('Producer flush did not complete within timeout')

