import datetime
import uuid
import random
from dataclasses import dataclass
from enum import Enum
from pprint import pprint
import factory
from factory import fuzzy
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


factory.Faker.add_provider(MsProvider)


# Factory Boy Factories
class TransactionFactory(factory.Factory):
    class Meta:
        model = Transaction

    transaction_id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    user_id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    amount = fuzzy.FuzzyFloat(1.0, 10000.0, precision=2)
    merchant = factory.Faker('company')
    currency = "USD"
    timestamp = factory.Faker('date_time_this_year_ms')
    is_fraud = fuzzy.FuzzyChoice([True, False])


class UserActivityFactory(factory.Factory):
    class Meta:
        model = UserActivity

    event_id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    user_id = factory.LazyFunction(lambda: str(uuid.uuid4()))
    event_type = fuzzy.FuzzyChoice([e.value for e in EventType])
    device = fuzzy.FuzzyChoice([d.value for d in Device])
    browser = fuzzy.FuzzyChoice([b.value for b in Browser])
    timestamp = factory.Faker('date_time_this_year_ms')


class EnhancedTransactionFactory(TransactionFactory):
    """Enhanced transaction factory with more realistic merchant names and fraud patterns"""

    merchant = fuzzy.FuzzyChoice([
        'Amazon', 'Target', 'Walmart', 'Best Buy', 'Home Depot',
        'Starbucks', 'McDonald\'s', 'Shell', 'Exxon', 'CVS Pharmacy',
        'Walgreens', 'Kroger', 'Safeway', 'Costco', 'Apple Store'
    ])

    amount = factory.LazyFunction(lambda:
                                  random.choices([
                                      round(random.uniform(5.0, 50.0), 2),  # 60%
                                      round(random.uniform(50.0, 200.0), 2),  # 25%
                                      round(random.uniform(200.0, 1000.0), 2),  # 10%
                                      round(random.uniform(1000.0, 5000.0), 2)  # 5%
                                  ], weights=[60, 25, 10, 5])[0]
                                  )

    is_fraud = factory.LazyFunction(lambda: random.random() < 0.01)


class EnhancedUserActivityFactory(UserActivityFactory):
    """Enhanced user activity factory with realistic device/browser combinations"""

    device = fuzzy.FuzzyChoice([d.value for d in Device])

    browser = factory.LazyAttribute(lambda obj: {
        'mobile': random.choices(['Chrome', 'Safari'], weights=[60, 40])[0],
        'desktop': random.choices(['Chrome', 'Firefox', 'Edge', 'Safari'], weights=[65, 15, 15, 5])[0],
        'tablet': random.choices(['Chrome', 'Safari'], weights=[45, 55])[0]
    }.get(obj.device, 'Chrome'))


class DataGenerator:
    """Utility class for generating related datasets"""

    @staticmethod
    def generate_user_session(user_id: str, num_activities: int = 5) -> list[UserActivity]:
        """Generate a sequence of activities for a single user session"""
        base_time = fake.date_time_this_month()
        activities = []

        for i in range(num_activities):
            activity_time = base_time.replace(
                minute=min(59, base_time.minute + i * random.randint(1, 5))
            )

            activity = UserActivityFactory(
                user_id=user_id,
                timestamp=int(activity_time.timestamp() * 1000)
            )
            activities.append(activity)
        return activities

    @staticmethod
    def generate_user_with_transactions(num_transactions: int = 10) -> tuple[str, list[Transaction]]:
        """Generate transactions for a single user"""
        user_id = str(uuid.uuid4())
        transactions = TransactionFactory.create_batch(
            num_transactions,
            user_id=user_id
        )
        return user_id, transactions

    @staticmethod
    def generate_complete_dataset(num_users: int = 100) -> dict:
        """Generate a complete dataset with users, transactions, and activities"""
        dataset = {
            'users': [],
            'transactions': [],
            'activities': []
        }

        for _ in range(num_users):
            user_id = str(uuid.uuid4())
            dataset['users'].append(user_id)

            num_transactions = random.randint(5, 20)
            user_transactions = EnhancedTransactionFactory.create_batch(
                num_transactions,
                user_id=user_id
            )
            dataset['transactions'].extend(user_transactions)

            num_activities = random.randint(10, 50)
            user_activities = EnhancedUserActivityFactory.create_batch(
                num_activities,
                user_id=user_id
            )
            dataset['activities'].extend(user_activities)

        return dataset
