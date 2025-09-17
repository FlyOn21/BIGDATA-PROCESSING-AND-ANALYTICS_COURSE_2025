import datetime
import json
import random
import uuid
from dataclasses import dataclass
from enum import Enum

import factory
from faker import Faker
from faker.providers import BaseProvider

fake = Faker()


class EventType(Enum):
    CLICK = "click"
    VIEW = "view"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"


class Device(Enum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"


class Browser(Enum):
    CHROME = "Chrome"
    SAFARI = "Safari"
    FIREFOX = "Firefox"
    EDGE = "Edge"


# Data Models
@dataclass
class Transaction:
    transaction_id: str
    user_id: str
    amount: float
    merchant: str
    currency: str
    timestamp: int
    is_fraud: bool


@dataclass
class UserActivity:
    event_id: str
    user_id: str
    event_type: str
    device: str
    browser: str
    timestamp: int


class MsProvider(BaseProvider):
    def date_time_this_year_ms(self):
        datetime_random = self.generator.date_time_this_year(tzinfo=datetime.UTC)
        return int(datetime_random.timestamp() * 1000)

    def recent_timestamp_ms(self, max_hours_ago=24):
        """Generate timestamp within last N hours"""
        now = datetime.datetime.now(datetime.UTC)
        hours_ago = random.uniform(0, max_hours_ago)
        target_time = now - datetime.timedelta(hours=hours_ago)
        return int(target_time.timestamp() * 1000)


factory.Faker.add_provider(MsProvider)


class FixedUserPool:
    """Manages a pool of fixed users with realistic behavior patterns"""

    def __init__(self, core_user_count: int = 25):
        self.core_user_count = core_user_count
        self.core_users = self._generate_core_users()
        self.user_profiles = self._create_user_profiles()

    def _generate_core_users(self) -> list[str]:
        """Generate fixed core user IDs"""
        return [str(uuid.uuid4()) for _ in range(self.core_user_count)]

    def _create_user_profiles(self) -> dict[str, dict]:
        """Create behavioral profiles for core users"""
        return {
            user_id: {
                'activity_level': random.choice(['low', 'medium', 'high']),
                'preferred_device': random.choice([d.value for d in Device]),
                'fraud_tendency': random.random() < 0.02,
                'spending_tier': random.choice(['budget', 'medium', 'premium']),
                'favorite_merchants': random.sample([
                    'Amazon', 'Target', 'Walmart', 'Best Buy', 'Home Depot',
                    'Starbucks', 'McDonald\'s', 'Shell', 'Exxon', 'CVS Pharmacy'
                ], k=3)
            }
            for user_id in self.core_users
        }

    def get_user_id(self, new_user_probability: float = 0.05) -> str:
        """Get user ID, mostly from core users, occasionally new users"""
        if random.random() < new_user_probability:
            return str(uuid.uuid4())  # New user
        return random.choice(self.core_users)

    def get_user_profile(self, user_id: str) -> dict:
        """Get user profile or default for new users"""
        return self.user_profiles.get(user_id, {
            'activity_level': 'medium',
            'preferred_device': random.choice([d.value for d in Device]),
            'fraud_tendency': False,
            'spending_tier': 'medium',
            'favorite_merchants': ['Amazon', 'Target', 'Walmart']
        })

USER_POOL = FixedUserPool(core_user_count=25)


class EnhancedTransactionFactory(factory.Factory):
    """Enhanced transaction factory with fixed user pool and realistic patterns"""

    class Meta:
        model = Transaction

    transaction_id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    user_id = factory.LazyFunction(lambda: USER_POOL.get_user_id(new_user_probability=0.05))
    currency = "USD"
    timestamp = factory.Faker('recent_timestamp_ms', max_hours_ago=6)

    @factory.lazy_attribute
    def merchant(self):
        profile = USER_POOL.get_user_profile(self.user_id)
        if random.random() < 0.7:
            return random.choice(profile['favorite_merchants'])
        else:
            return random.choice([
                'Amazon', 'Target', 'Walmart', 'Best Buy', 'Home Depot',
                'Starbucks', 'McDonald\'s', 'Shell', 'Exxon', 'CVS Pharmacy',
                'Walgreens', 'Kroger', 'Safeway', 'Costco', 'Apple Store'
            ])

    @factory.lazy_attribute
    def amount(self):
        profile = USER_POOL.get_user_profile(self.user_id)
        spending_tier = profile['spending_tier']

        if spending_tier == 'budget':
            return round(random.uniform(5.0, 150.0), 2)
        elif spending_tier == 'premium':
            return round(random.uniform(100.0, 2000.0), 2)
        else:
            return round(random.uniform(20.0, 500.0), 2)

    @factory.lazy_attribute
    def is_fraud(self):
        profile = USER_POOL.get_user_profile(self.user_id)
        base_fraud_rate = 0.01
        if profile['fraud_tendency']:
            base_fraud_rate *= 3
        return random.random() < base_fraud_rate


class EnhancedUserActivityFactory(factory.Factory):
    """Enhanced user activity factory with fixed user pool and realistic patterns"""

    class Meta:
        model = UserActivity

    event_id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    user_id = factory.LazyFunction(lambda: USER_POOL.get_user_id(new_user_probability=0.03))
    timestamp = factory.Faker('recent_timestamp_ms', max_hours_ago=6)

    @factory.lazy_attribute
    def device(self):
        profile = USER_POOL.get_user_profile(self.user_id)
        preferred = profile['preferred_device']
        if random.random() < 0.6:
            return preferred
        else:
            return random.choice([d.value for d in Device])

    @factory.lazy_attribute
    def browser(self):
        device_browser_map = {
            'mobile': random.choices(['Chrome', 'Safari'], weights=[60, 40])[0],
            'desktop': random.choices(['Chrome', 'Firefox', 'Edge', 'Safari'], weights=[65, 15, 15, 5])[0],
            'tablet': random.choices(['Chrome', 'Safari'], weights=[45, 55])[0]
        }
        return device_browser_map.get(self.device, 'Chrome')

    @factory.lazy_attribute
    def event_type(self):
        profile = USER_POOL.get_user_profile(self.user_id)
        activity_level = profile['activity_level']

        if activity_level == 'high':
            weights = [30, 25, 25, 20]
        elif activity_level == 'low':
            weights = [50, 40, 8, 2]
        else:  # medium
            weights = [40, 35, 20, 5]

        return random.choices([e.value for e in EventType], weights=weights)[0]


class EnhancedDataGenerator:
    """Enhanced data generator with user session correlation and realistic patterns"""

    def __init__(self):
        self.user_pool = USER_POOL

    def generate_user_session(self, user_id: str = None, session_duration_minutes: int = 30) -> list[UserActivity]:
        """Generate a realistic user session with correlated activities"""
        if user_id is None:
            user_id = self.user_pool.get_user_id()

        profile = self.user_pool.get_user_profile(user_id)
        activity_level = profile['activity_level']

        # Determine session size based on activity level
        if activity_level == 'high':
            num_activities = random.randint(10, 25)
        elif activity_level == 'low':
            num_activities = random.randint(2, 8)
        else:
            num_activities = random.randint(5, 15)

        base_time = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
            hours=random.uniform(0, 6)
        )

        activities = []
        current_time = base_time

        for _ in range(num_activities):
            time_increment = random.uniform(30, 300)
            current_time += datetime.timedelta(seconds=time_increment)

            activity = EnhancedUserActivityFactory(
                user_id=user_id,
                timestamp=int(current_time.timestamp() * 1000)
            )
            activities.append(activity)

        return activities

    def generate_correlated_transaction_activity(self, user_id: str = None) -> dict[str, list]:
        """Generate correlated transactions and activities for a user"""
        if user_id is None:
            user_id = self.user_pool.get_user_id()

        profile = self.user_pool.get_user_profile(user_id)

        # Generate transactions
        if profile['spending_tier'] == 'premium':
            num_transactions = random.randint(5, 15)
        elif profile['spending_tier'] == 'budget':
            num_transactions = random.randint(1, 8)
        else:  # medium
            num_transactions = random.randint(2, 10)

        transactions = []
        activities = []

        for _ in range(num_transactions):
            transaction = EnhancedTransactionFactory(user_id=user_id)
            transactions.append(transaction)

            if random.random() < 0.7:
                pre_activities = self.generate_user_session(user_id, 15)
                activities.extend(pre_activities)

        standalone_activities = self.generate_user_session(user_id, 20)
        activities.extend(standalone_activities)

        return {
            'user_id': user_id,
            'transactions': transactions,
            'activities': activities
        }

    def generate_realistic_batch(self, batch_size: int = 1000) -> dict[str, list]:
        """Generate a realistic batch with high correlation between core users"""
        transactions = []
        activities = []

        core_user_data_ratio = 0.8
        core_data_size = int(batch_size * core_user_data_ratio)

        core_users_sample = random.sample(self.user_pool.core_users,
                                          min(15, len(self.user_pool.core_users)))

        for _ in range(core_data_size):
            user_id = random.choice(core_users_sample)
            if random.random() < 0.6:
                transaction = EnhancedTransactionFactory(user_id=user_id)
                transactions.append(transaction)
            else:
                activity = EnhancedUserActivityFactory(user_id=user_id)
                activities.append(activity)

        # Generate remaining data (mixed users)
        remaining_size = batch_size - core_data_size
        for _ in range(remaining_size):
            if random.random() < 0.4:
                transaction = EnhancedTransactionFactory()
                transactions.append(transaction)
            else:
                activity = EnhancedUserActivityFactory()
                activities.append(activity)

        return {
            'transactions': transactions,
            'activities': activities
        }

    def get_core_users(self) -> list[str]:
        """Get list of core user IDs"""
        return self.user_pool.core_users.copy()

    def save_user_pool_info(self, filepath: str = 'user_pool_info.json'):
        """Save user pool information for reference"""
        info = {
            'core_users': self.user_pool.core_users,
            'user_profiles': self.user_pool.user_profiles,
            'total_core_users': len(self.user_pool.core_users)
        }

        with open(filepath, 'w') as f:
            json.dump(info, f, indent=2)

        print(f"User pool info saved to {filepath}")
        print(f"Core users: {len(self.user_pool.core_users)}")

DATA_GENERATOR = EnhancedDataGenerator()

__all__ = [
    'EnhancedTransactionFactory',
    'EnhancedUserActivityFactory',
    'EnhancedDataGenerator',
    'DATA_GENERATOR',
    'USER_POOL'
]
