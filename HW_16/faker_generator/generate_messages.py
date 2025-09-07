import argparse
import json
import logging
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from typing import Any, Dict

from fake_data_generator import DATA_GENERATOR, USER_POOL, EnhancedTransactionFactory
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
from kafka.producer.future import FutureRecordMetadata
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EnhancedKafkaDataProducer:
    """Enhanced Kafka producer with fixed user correlation and realistic patterns"""

    def __init__(self,
                 bootstrap_servers: str = '127.0.0.1:9092',
                 batch_size: int = 1000,
                 linger_ms: int = 100):
        """Initialize producer with optimized settings"""

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
            buffer_memory=64 * 1024 * 1024,
            request_timeout_ms=30000,
            delivery_timeout_ms=60000
        )

        self.data_generator = DATA_GENERATOR
        self.stats = {
            'transactions_sent': 0,
            'activities_sent': 0,
            'errors': 0,
            'bytes_sent': 0,
            'core_user_transactions': 0,
            'core_user_activities': 0,
            'new_user_data': 0
        }

        # Thread-safe counter
        self._lock = threading.Lock()

        logger.info(f"Producer initialized with {len(USER_POOL.core_users)} core users")

    def _update_stats(self, **kwargs):
        """Thread-safe stats update"""
        with self._lock:
            for key, value in kwargs.items():
                if key in self.stats:
                    self.stats[key] += value

    def _send_message(
            self,
            payload: dict[str, Any],
            topic: str,
            count_key: str,
            core_count_key: str
    )-> FutureRecordMetadata | None:
        try:
            key = payload.get('user_id', str(uuid.uuid4()))
            future = self.producer.send(topic=topic, key=key, value=payload)
            size = len(json.dumps(payload, default=str))
            is_core = key in USER_POOL.core_users

            self._update_stats(**{
                count_key: 1,
                'bytes_sent': size,
                core_count_key: 1 if is_core else 0,
                'new_user_data': 0 if is_core else 1
            })
            return future

        except KafkaError as e:
            logger.error(f"Failed to send to {topic}: {e}")
            self._update_stats(errors=1)
            return None

    def send_transaction(self, tx: dict, topic: str = 'transactions') -> FutureRecordMetadata | None:
        return self._send_message(
            payload=tx,
            topic=topic,
            count_key='transactions_sent',
            core_count_key='core_user_transactions'
        )

    def send_activity(self, act: dict, topic: str = 'user_activity')-> FutureRecordMetadata | None:
        return self._send_message(
            payload=act,
            topic=topic,
            count_key='activities_sent',
            core_count_key='core_user_activities'
        )

    def generate_realistic_batch(self, batch_size: int = 1000):
        """Generate realistic batch using enhanced data generator"""
        try:
            batch_data = self.data_generator.generate_realistic_batch(batch_size)
            for transaction in batch_data['transactions']:
                self.send_transaction(asdict(transaction))

            for activity in batch_data['activities']:
                self.send_activity(asdict(activity))

            self.producer.flush()

        except Exception as e:
            logger.error(f"Error generating realistic batch: {e}")

    def generate_user_sessions(self, num_sessions: int = 50):
        """Generate correlated user sessions"""
        try:
            core_users = random.sample(USER_POOL.core_users,
                                       min(num_sessions // 2, len(USER_POOL.core_users)))

            for user_id in core_users:
                session_activities = self.data_generator.generate_user_session(user_id)
                for activity in session_activities:
                    self.send_activity(asdict(activity))

                if random.random() < 0.3:
                    transaction = EnhancedTransactionFactory(user_id=user_id)
                    self.send_transaction(asdict(transaction))

            remaining_sessions = num_sessions - len(core_users)
            for _ in range(remaining_sessions):
                session_activities = self.data_generator.generate_user_session()
                for activity in session_activities:
                    self.send_activity(asdict(activity))

            self.producer.flush()

        except Exception as e:
            logger.error(f"Error generating user sessions: {e}")

    def generate_correlated_data_burst(self, num_users: int = 20):
        """Generate burst of correlated transaction and activity data"""
        try:
            selected_users = random.sample(USER_POOL.core_users,
                                           min(num_users, len(USER_POOL.core_users)))

            for user_id in selected_users:
                correlated_data = self.data_generator.generate_correlated_transaction_activity(user_id)

                for transaction in correlated_data['transactions']:
                    self.send_transaction(asdict(transaction))
                for activity in correlated_data['activities']:
                    self.send_activity(asdict(activity))

            self.producer.flush()

        except Exception as e:
            logger.error(f"Error generating correlated data burst: {e}")

    def generate_streaming_data_enhanced(self,
                                         target_size_gb: float = 2.0,
                                         batch_size: int = 1000,
                                         sleep_interval: float = 0.05) -> None:

        """Enhanced streaming data generation with realistic patterns"""
        target_bytes = target_size_gb * 1024 * 1024 * 1024

        pbar = tqdm(total=target_bytes, unit='B', unit_scale=True,
                    desc="Generating enhanced data")

        try:
            iteration = 0
            while self.stats['bytes_sent'] < target_bytes:
                prev_bytes = self.stats['bytes_sent']

                pattern = random.random()

                if pattern < 0.6:  # 60%
                    self.generate_realistic_batch(batch_size)
                elif pattern < 0.8:  # 20%
                    self.generate_user_sessions(num_sessions=batch_size // 20)
                else:
                    self.generate_correlated_data_burst(num_users=batch_size // 50)

                bytes_sent = self.stats['bytes_sent'] - prev_bytes
                pbar.update(bytes_sent)

                if sleep_interval > 0:
                    time.sleep(sleep_interval)

                iteration += 1
                if iteration % 50 == 0:
                    self.log_enhanced_statistics()

        except KeyboardInterrupt:
            logger.info("Generation interrupted by user")
        finally:
            pbar.close()
            self.producer.flush()
            self.producer.close()
            self.log_enhanced_statistics()

    def generate_parallel_enhanced_data(self,
                                        target_size_gb: float = 2.0,
                                        num_threads: int = 4):
        """Enhanced parallel data generation"""
        target_bytes = target_size_gb * 1024 * 1024 * 1024
        bytes_per_thread = target_bytes / num_threads

        def worker(thread_id: int):
            """Enhanced worker function"""
            thread_bytes = 0

            while thread_bytes < bytes_per_thread:
                prev_bytes = self.stats['bytes_sent']
                if thread_id % 3 == 0:
                    self.generate_realistic_batch(batch_size=300)
                elif thread_id % 3 == 1:
                    self.generate_user_sessions(num_sessions=15)
                else:
                    self.generate_correlated_data_burst(num_users=10)

                thread_bytes += (self.stats['bytes_sent'] - prev_bytes)

                time.sleep(0.01)
                if thread_id == 0 and self.stats['transactions_sent'] % 5000 == 0:
                    mb_sent = self.stats['bytes_sent'] / 1024 / 1024
                    core_ratio = (self.stats['core_user_transactions'] + self.stats['core_user_activities']) / max(1,
                                                                                                                   self.stats[
                                                                                                                       'transactions_sent'] +
                                                                                                                   self.stats[
                                                                                                                       'activities_sent'])
                    logger.info(f"Progress: {mb_sent:.1f} MB sent, {core_ratio:.1%} from core users")

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            for future in futures:
                future.result()

        self.producer.flush()
        self.producer.close()
        self.log_enhanced_statistics()

    def log_enhanced_statistics(self):
        """Enhanced statistics logging"""
        total_messages = self.stats['transactions_sent'] + self.stats['activities_sent']
        core_user_data = self.stats['core_user_transactions'] + self.stats['core_user_activities']

        core_user_ratio = core_user_data / max(1, total_messages)
        mb_sent = self.stats['bytes_sent'] / 1024 / 1024

        logger.info(f"""
        Enhanced Statistics:
        - Transactions sent: {self.stats['transactions_sent']:,}
        - Activities sent: {self.stats['activities_sent']:,}
        - Total messages: {total_messages:,}
        - Data sent: {mb_sent:.2f} MB
        - Core user data: {core_user_data:,} ({core_user_ratio:.1%})
        - New user data: {self.stats['new_user_data']:,}
        - Errors: {self.stats['errors']}
        - Core users active: {len(USER_POOL.core_users)}
        """)

    def save_generation_report(self, filepath: str = 'generation_report.json'):
        """Save detailed generation report"""
        report = {
            'timestamp': time.time(),
            'stats': self.stats.copy(),
            'core_users': USER_POOL.core_users,
            'total_core_users': len(USER_POOL.core_users),
            'generation_settings': {
                'core_user_ratio': (self.stats['core_user_transactions'] + self.stats['core_user_activities']) / max(1,
                                                                                                                     self.stats[
                                                                                                                         'transactions_sent'] +
                                                                                                                     self.stats[
                                                                                                                         'activities_sent']),
                'data_size_mb': self.stats['bytes_sent'] / 1024 / 1024
            }
        }

        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2)

        logger.info(f"Generation report saved to {filepath}")


def create_optimized_topics():
    """Create optimized Kafka topics"""
    admin_client = KafkaAdminClient(
        bootstrap_servers='127.0.0.1:9092',
        client_id='enhanced_topic_creator'
    )

    topic_configs = [
        {
            'name': 'transactions',
            'partitions': 6,
            'replication_factor': 1,
            'config': {
                'retention.ms': '604800000',  # 7 days
                'compression.type': 'snappy',
                'segment.ms': '1800000',  # 30 minutes
                'cleanup.policy': 'delete',
                'message.timestamp.type': 'CreateTime',
                'min.insync.replicas': '1',
                'unclean.leader.election.enable': 'false'
            }
        },
        {
            'name': 'user_activity',
            'partitions': 6,
            'replication_factor': 1,
            'config': {
                'retention.ms': '604800000',  # 7 days
                'compression.type': 'snappy',
                'segment.ms': '1800000',  # 30 minutes
                'cleanup.policy': 'delete',
                'message.timestamp.type': 'CreateTime',
                'min.insync.replicas': '1',
                'unclean.leader.election.enable': 'false'
            }
        }
    ]

    topics = []
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
        logger.info(f"Created optimized topics: {[t.name for t in topics]}")
    except Exception as e:
        logger.warning(f"Topics might already exist: {e}")

    admin_client.close()


def main() -> None:
    """Enhanced main execution function"""
    parser = argparse.ArgumentParser(description='Enhanced Kafka data generation with fixed users')
    parser.add_argument('--size-gb', type=float, default=2.0,
                        help='Target data size in GB (default: 2.0)')
    parser.add_argument('--batch-size', type=int, default=1000,
                        help='Messages per batch (default: 1000)')
    parser.add_argument('--parallel', action='store_true',
                        help='Use parallel generation')
    parser.add_argument('--threads', type=int, default=4,
                        help='Number of threads for parallel generation')
    parser.add_argument('--bootstrap-servers', type=str, default='127.0.0.1:9092',
                        help='Kafka bootstrap servers')
    parser.add_argument('--create-topics', action='store_true', default=False,
                        help='Create topics before starting')
    parser.add_argument('--core-users', type=int, default=25,
                        help='Number of core users to generate')
    parser.add_argument('--sleep-interval', type=float, default=0.02,
                        help='Sleep interval between batches')

    args = parser.parse_args()

    # Create topics if requested
    if args.create_topics:
        create_optimized_topics()
        time.sleep(2)

    # Initialize and save user pool info
    DATA_GENERATOR.save_user_pool_info()

    logger.info(f"""
    Starting enhanced data generation:
    - Target size: {args.size_gb} GB
    - Core users: {len(USER_POOL.core_users)}
    - Parallel: {args.parallel}
    - Threads: {args.threads if args.parallel else 1}
    """)

    producer = EnhancedKafkaDataProducer(bootstrap_servers=args.bootstrap_servers)

    try:
        if args.parallel:
            logger.info(f"Using enhanced parallel generation with {args.threads} threads")
            producer.generate_parallel_enhanced_data(
                target_size_gb=args.size_gb,
                num_threads=args.threads
            )
        else:
            logger.info("Using enhanced sequential generation")
            producer.generate_streaming_data_enhanced(
                target_size_gb=args.size_gb,
                batch_size=args.batch_size,
                sleep_interval=args.sleep_interval
            )

        # Save generation report
        producer.save_generation_report()

    except Exception as e:
        logger.error(f"Error during enhanced data generation: {e}")
        raise e


if __name__ == "__main__":
    main()
