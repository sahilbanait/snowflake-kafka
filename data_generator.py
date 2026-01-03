import sys
import rapidjson as json
import optional_faker as _
import uuid
import random

from dotenv import load_dotenv
from faker import Faker
from datetime import date, datetime
import os
import logging
import confluent_kafka
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

load_dotenv()
logging.basicConfig(level=logging.INFO)

kafka_brokers = os.getenv("REDPANDA_BROKERS")
topic_name = os.getenv("KAFKA_TOPIC")

fake = Faker()
resorts = ["Vail", "Beaver Creek", "Breckenridge", "Keystone", "Crested Butte", "Park City", "Heavenly", "Northstar",
           "Kirkwood", "Whistler Blackcomb", "Perisher", "Falls Creek", "Hotham", "Stowe", "Mount Snow", "Okemo",
           "Hunter Mountain", "Mount Sunapee", "Attitash", "Wildcat", "Crotched", "Stevens Pass", "Liberty", "Roundtop", 
           "Whitetail", "Jack Frost", "Big Boulder", "Alpine Valley", "Boston Mills", "Brandywine", "Mad River",
           "Hidden Valley", "Snow Creek", "Wilmot", "Afton Alps" , "Mt. Brighton", "Paoli Peaks"]    
 


def _require_env(var, name):
    if not var:
        logging.error(f"Environment variable {name} is not set. Please set it in .env or your environment.")
        raise SystemExit(1)


def create_topic(retries: int = 3):
    # Validate required env settings
    _require_env(kafka_brokers, 'REDPANDA_BROKERS')
    _require_env(topic_name, 'KAFKA_TOPIC')

    admin_client = KafkaAdminClient(bootstrap_servers=kafka_brokers, client_id='data_generator')

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
        except TopicAlreadyExistsError:
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


def generate_lift_ticket():
    global resorts, fake
    state = fake.state_abbr()
    lift_ticket = {'txid': str(uuid.uuid4()),
                   'rfid': hex(random.getrandbits(96)),
                   'resort': fake.random_element(elements=resorts),
                   'purchase_time': datetime.utcnow().isoformat(),
                   'expiration_time': date(2023, 6, 1).isoformat(),
                   'days': fake.random_int(min=1, max=7),
                   'name': fake.name(),
                   'address': fake.none_or({'street_address': fake.street_address(), 
                                             'city': fake.city(), 'state': state, 
                                             'postalcode': fake.postalcode_in_state(state)}),
                   'phone': fake.none_or(fake.phone_number()),
                   'email': fake.none_or(fake.email()),
                   'emergency_contact' : fake.none_or({'name': fake.name(), 'phone': fake.phone_number()}),
    }
    # Add a newline so each JSON message is a separate line for streaming/piping
    return json.dumps(lift_ticket)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python data_generator.py <count>")
        sys.exit(1)
        
    args = sys.argv[1:]
    total_count = int(args[0])
    
    producer = get_kafka_producer()
    create_topic()
    
    for _ in range(total_count):
        ticket = generate_lift_ticket()
        producer.produce(topic_name, value=ticket.encode('utf-8'))
        
    producer.flush()

