import argparse
import json
import logging
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from typing import Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError
from tqdm import tqdm

from fake_data_generator import (
    EnhancedTransactionFactory,
    EnhancedUserActivityFactory
)
from kafka.admin import KafkaAdminClient, NewTopic

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KafkaDataProducer:
    """Kafka producer for streaming generated data"""

    def __init__(self,
                 bootstrap_servers: str = '127.0.0.1:9092',
                 batch_size: int = 1000,
                 linger_ms: int = 100):
        """
        Initialize Kafka producer with optimized settings for high throughput

        Args:
            bootstrap_servers: Kafka broker addresses
            batch_size: Maximum batch size in bytes
            linger_ms: Time to wait for batching messages
        """
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            compression_type='snappy',
            batch_size=batch_size * 1024,
            linger_ms=linger_ms,
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=5,
            buffer_memory=64 * 1024 * 1024
        )

        self.stats = {
            'transactions_sent': 0,
            'activities_sent': 0,
            'errors': 0,
            'bytes_sent': 0
        }

    def send_transaction(self, transaction: Dict[str, Any], topic: str = 'transactions'):
        """Send transaction to Kafka"""
        try:
            key = transaction.get('user_id', str(uuid.uuid4()))

            # Send message
            future = self.producer.send(
                topic=topic,
                key=key,
                value=transaction
            )

            # Update statistics
            self.stats['transactions_sent'] += 1
            self.stats['bytes_sent'] += len(json.dumps(transaction, default=str))

            return future

        except KafkaError as e:
            logger.error(f"Failed to send transaction: {e}")
            self.stats['errors'] += 1
            return None

    def send_activity(self, activity: Dict[str, Any], topic: str = 'user_activity'):
        """Send user activity to Kafka"""
        try:
            key = activity.get('user_id', str(uuid.uuid4()))

            future = self.producer.send(
                topic=topic,
                key=key,
                value=activity
            )
            self.stats['activities_sent'] += 1
            self.stats['bytes_sent'] += len(json.dumps(activity, default=str))

            return future

        except KafkaError as e:
            logger.error(f"Failed to send activity: {e}")
            self.stats['errors'] += 1
            return None

    def generate_and_send_batch(self,
                                batch_size: int = 1000,
                                transaction_ratio: float = 0.3):
        """
        Generate and send a batch of messages

        Args:
            batch_size: Number of messages in batch
            transaction_ratio: Ratio of transactions vs activities
        """
        num_transactions = int(batch_size * transaction_ratio)
        num_activities = batch_size - num_transactions

        # Generate transactions
        for _ in range(num_transactions):
            transaction = EnhancedTransactionFactory()
            self.send_transaction(asdict(transaction))

        # Generate activities
        for _ in range(num_activities):
            activity = EnhancedUserActivityFactory()
            self.send_activity(asdict(activity))

        self.producer.flush()

    def generate_correlated_data(self, num_users: int = 100):
        """
        Generate correlated data for users (transactions and activities from same users)
        """
        for _ in range(num_users):
            user_id = str(uuid.uuid4())
            num_transactions = random.randint(5, 20)
            for _ in range(num_transactions):
                transaction = EnhancedTransactionFactory(user_id=user_id)
                self.send_transaction(asdict(transaction))

            num_activities = random.randint(10, 50)
            for _ in range(num_activities):
                activity = EnhancedUserActivityFactory(user_id=user_id)
                self.send_activity(asdict(activity))

        self.producer.flush()

    def generate_streaming_data(self,
                                target_size_gb: float = 2.0,
                                batch_size: int = 1000,
                                sleep_interval: float = 0.1):
        """
        Continuously generate data until target size is reached

        Args:
            target_size_gb: Target data size in GB
            batch_size: Messages per batch
            sleep_interval: Sleep time between batches (seconds)
        """
        target_bytes = target_size_gb * 1024 * 1024 * 1024

        pbar = tqdm(total=target_bytes, unit='B', unit_scale=True)

        try:
            while self.stats['bytes_sent'] < target_bytes:
                prev_bytes = self.stats['bytes_sent']

                if random.random() < 0.8:
                    self.generate_and_send_batch(batch_size)
                else:
                    self.generate_correlated_data(num_users=10)

                # Update progress bar
                bytes_sent = self.stats['bytes_sent'] - prev_bytes
                pbar.update(bytes_sent)

                #sleep to control rate
                if sleep_interval > 0:
                    time.sleep(sleep_interval)

                # Log progress periodically
                if self.stats['transactions_sent'] % 10000 == 0:
                    self.log_statistics()

        except KeyboardInterrupt:
            logger.info("Generation interrupted by user")

        finally:
            pbar.close()
            self.producer.flush()
            self.producer.close()
            self.log_statistics()

    def generate_parallel_data(self,
                               target_size_gb: float = 2.0,
                               num_threads: int = 4):
        """
        Generate data using multiple threads for higher throughput

        Args:
            target_size_gb: Target data size in GB
            num_threads: Number of parallel threads
        """
        target_bytes = target_size_gb * 1024 * 1024 * 1024
        bytes_per_thread = target_bytes / num_threads

        def worker(thread_id: int):
            """Worker function for parallel generation"""
            thread_bytes = 0

            while thread_bytes < bytes_per_thread:
                prev_bytes = self.stats['bytes_sent']

                if random.random() < 0.8:
                    self.generate_and_send_batch(batch_size=500)
                else:
                    self.generate_correlated_data(num_users=20)

                thread_bytes += (self.stats['bytes_sent'] - prev_bytes)

                #progress bar
                if thread_id == 0 and self.stats['transactions_sent'] % 10000 == 0:
                    logger.info(f"Progress: {self.stats['bytes_sent'] / 1024 / 1024:.2f} MB sent")

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            for future in futures:
                future.result()

        self.producer.flush()
        self.producer.close()
        self.log_statistics()

    def log_statistics(self):
        """Log current statistics"""
        logger.info(f"""
        Statistics:
        - Transactions sent: {self.stats['transactions_sent']:,}
        - Activities sent: {self.stats['activities_sent']:,}
        - Total messages: {self.stats['transactions_sent'] + self.stats['activities_sent']:,}
        - Data sent: {self.stats['bytes_sent'] / 1024 / 1024:.2f} MB
        - Errors: {self.stats['errors']}
        """)


def create_topics_if_needed():
    """Create Kafka topics if they don't exist"""
    admin_client = KafkaAdminClient(
        bootstrap_servers='127.0.0.1:9092',
        client_id='topic_creator'
    )

    topics = []

    topic_configs = [
    {
        'name': 'transactions',
        'partitions': 6,
        'replication_factor': 1,
        'config': {
            'retention.ms': '604800000',
            'compression.type': 'snappy',
            'segment.ms': '3600000',
            'cleanup.policy': 'delete',
            'message.timestamp.type': 'CreateTime',
            'message.timestamp.difference.max.ms': '604800000',
        }
    },
    {
        'name': 'user_activity',
        'partitions': 6,
        'replication_factor': 1,
        'config': {
            'retention.ms': '604800000',
            'compression.type': 'snappy',
            'segment.ms': '3600000',
            'cleanup.policy': 'delete',
            'message.timestamp.type': 'CreateTime',
            'message.timestamp.difference.max.ms': '604800000'
        }
    }
]

    for config in topic_configs:
        topic = NewTopic(
            name=config['name'],
            num_partitions=config['partitions'],
            replication_factor=config['replication_factor'],
            topic_configs=config['config']
        )
        topics.append(topic)

    try:
        admin_client.create_topics(topics, validate_only=False)
        logger.info(f"Created topics: {[t.name for t in topics]}")
    except Exception as e:
        logger.warning(f"Topics might already exist: {e}")

    admin_client.close()



def main() -> None:
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Generate and send data to Kafka and/or Parquet')
    parser.add_argument(
        '--size-gb',
        type=float,
        default=2.0,
        help='Target data size in GB (default: 2.0)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Messages per batch (default: 1000)'
    )
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Use parallel generation for higher throughput',
        default=False
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=4,
        help='Number of threads for parallel generation (default: 4)'
    )
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='127.0.0.1:9092',
        help='Kafka bootstrap servers (default: 127.0.0.1:9092)'
    )
    parser.add_argument(
        '--create-topics',
        type=bool,
        default=True,
        help='Create topics before starting',
    )

    args = parser.parse_args()
    if args.create_topics:
        create_topics_if_needed()
        time.sleep(2)
    logger.info(f"Starting data generation. Target size: {args.size_gb} GB")
    producer = KafkaDataProducer(bootstrap_servers=args.bootstrap_servers)

    try:
        if args.parallel:
            logger.info(f"Using parallel generation with {args.threads} threads")
            producer.generate_parallel_data(
                target_size_gb=args.size_gb,
                num_threads=args.threads
            )
        else:
            logger.info("Using sequential generation")
            producer.generate_streaming_data(
                target_size_gb=args.size_gb,
                batch_size=args.batch_size,
                sleep_interval=0.01
            )
    except Exception as e:
        logger.error(f"Error during data generation: {e}")
        raise e


if __name__ == "__main__":
    main()