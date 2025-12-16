"""
Kafka configuration and helper utilities.
"""
import json
from typing import Type, TypeVar
from pydantic import BaseModel

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']

# Topic Names
TOPIC_BUY_ORDERS = 'buy-orders'
TOPIC_SELL_OFFERS = 'sell-offers'
TOPIC_DEMAT_TRANSFERS = 'demat-transfers'
TOPIC_CASH_MOVEMENTS = 'cash-movements'

# Consumer Group IDs
CONSUMER_GROUP_NSE = 'nse-consumer-group'
CONSUMER_GROUP_HDFC = 'hdfc-consumer-group'

T = TypeVar('T', bound=BaseModel)


def serialize_message(obj: BaseModel) -> bytes:
    """Serialize a Pydantic model to JSON bytes for Kafka"""
    return json.dumps(obj.model_dump()).encode('utf-8')


def deserialize_message(data: bytes, model_class: Type[T]) -> T:
    """Deserialize JSON bytes from Kafka to a Pydantic model"""
    json_data = json.loads(data.decode('utf-8'))
    return model_class(**json_data)
