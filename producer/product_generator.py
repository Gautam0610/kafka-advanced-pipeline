import os
import json
import random
import time
import logging
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

PRODUCT_CATEGORIES = {
    "Electronics": ['Laptop Dell XPS', 'iPhone 15 Pro', 'Samsung Galaxy Tab', 'Sony Headphones WH-1000XM5', 'Canon Camera EOS R5'],
    "Clothing": ['Nike Running Shoes', 'Levis Jeans 501', 'Adidas Hoodie', 'Zara Cotton Shirt', 'Puma Tracksuit'],
    "Home_Appliances": ['Dyson Vacuum V15', 'Instant Pot Duo', 'Ninja Blender', 'Philips Air Fryer', 'Nespresso Coffee Machine'],
    "Books": ['The Great Gatsby', 'Python Crash Course', '1984 George Orwell', 'Clean Code', 'Atomic Habits']
}

class ProductProducer:
    def __init__(self, config: dict):
        self.config = config
        self.producer = None
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config['BOOTSTRAP_SERVERS'].split(','),
                security_protocol=self.config.get('SECURITY_PROTOCOL', 'PLAINTEXT'),
                sasl_mechanism=self.config.get('SASL_MECHANISM'),
                sasl_plain_username=self.config.get('SASL_PLAIN_USERNAME'),
                sasl_plain_password=self.config.get('SASL_PLAIN_PASSWORD'),
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info("KafkaProducer initialized successfully.")
        except KafkaError as e:
            logger.error(f"Error initializing KafkaProducer: {e}")
            sys.exit(1)

    def generate_product(self) -> dict:
        category = random.choice(list(PRODUCT_CATEGORIES.keys()))
        product_name = random.choice(PRODUCT_CATEGORIES[category])
        
        product = {
            "product_id": random.randint(1000, 9999),
            "product_name": product_name,
            "category": category,
            "price": round(random.uniform(10.0, 999.99), 2),
            "timestamp": time.time(),
            "stock_quantity": random.randint(0, 100)
        }
        return product

    def send_product(self, product: dict):
        topic = self.config['OUTPUT_TOPIC']
        try:
            future = self.producer.send(topic, product)
            record_metadata = future.get(timeout=10) # Ensure delivery
            logger.info(f"Successfully sent product {product['product_id']} to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
        except KafkaError as e:
            logger.error(f"Failed to send product {product['product_id']} to topic {topic}: {e}")
        except Exception as e:
            logger.error(f"An unexpected error occurred while sending product {product['product_id']}: {e}")

    def run(self):
        try:
            while True:
                product = self.generate_product()
                self.send_product(product)
                logger.info(f"Product sent: {product['product_id']}")
                time.sleep(random.uniform(1, 3))
        except KeyboardInterrupt:
            logger.info("Producer shutting down gracefully...")
        finally:
            if self.producer:
                self.producer.close()
                logger.info("KafkaProducer closed.")

if __name__ == '__main__':
    load_dotenv()
    config = {
        'BOOTSTRAP_SERVERS': os.getenv('BOOTSTRAP_SERVERS', 'localhost:9092'),
        'OUTPUT_TOPIC': os.getenv('OUTPUT_TOPIC', 'products_producer'),
        'SECURITY_PROTOCOL': os.getenv('SECURITY_PROTOCOL', 'PLAINTEXT'),
        'SASL_MECHANISM': os.getenv('SASL_MECHANISM'),
        'SASL_PLAIN_USERNAME': os.getenv('SASL_PLAIN_USERNAME'),
        'SASL_PLAIN_PASSWORD': os.getenv('SASL_PLAIN_PASSWORD')
    }
    
    producer_app = ProductProducer(config)
    producer_app.run()