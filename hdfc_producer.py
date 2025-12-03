"""
HDFC Producer - Sends buy orders to Kafka.
Acts as a stock buyer sending order requests to NSE.
"""
import sys
import uuid
from kafka import KafkaProducer
from kafka.errors import KafkaError
from models import BuyOrder
from kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_BUY_ORDERS,
    serialize_message
)


class HDFCProducer:
    """HDFC producer for sending buy orders"""

    def __init__(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: v  # We handle serialization ourselves
            )
            print(f"✓ HDFC Producer initialized and connected to Kafka")
        except KafkaError as e:
            print(f"✗ Failed to initialize HDFC Producer: {e}")
            sys.exit(1)

    def send_buy_order(self, stock_symbol: str, quantity: int, price_per_unit: float):
        """
        Send a buy order to Kafka.

        Args:
            stock_symbol: Stock symbol (e.g., 'SBI')
            quantity: Number of stocks to buy
            price_per_unit: Price per stock unit
        """
        order = BuyOrder(
            order_id=str(uuid.uuid4()),
            buyer="HDFC",
            stock_symbol=stock_symbol,
            quantity=quantity,
            price_per_unit=price_per_unit
        )

        try:
            # Serialize and send to Kafka
            message = serialize_message(order)
            future = self.producer.send(TOPIC_BUY_ORDERS, value=message)
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            print(f"\n{'='*60}")
            print(f"📤 HDFC: Buy order sent successfully!")
            print(f"{'='*60}")
            print(f"  Order ID: {order.order_id}")
            print(f"  Stock: {order.stock_symbol}")
            print(f"  Quantity: {order.quantity}")
            print(f"  Price per unit: ₹{order.price_per_unit:,.2f}")
            print(f"  Total amount: ₹{order.total_amount:,.2f}")
            print(f"  Topic: {record_metadata.topic}")
            print(f"  Partition: {record_metadata.partition}")
            print(f"  Offset: {record_metadata.offset}")
            print(f"{'='*60}\n")
            
            return order.order_id

        except KafkaError as e:
            print(f"✗ Failed to send buy order: {e}")
            return None

    def close(self):
        """Close the producer connection"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            print("✓ HDFC Producer connection closed")


def main():
    """Main function to demonstrate HDFC producer"""
    producer = HDFCProducer()
    
    # Send buy order for 100 SBI stocks at ₹885
    producer.send_buy_order(
        stock_symbol="SBI",
        quantity=500,
        price_per_unit=885.0
    )
    
    producer.close()


if __name__ == "__main__":
    main()
