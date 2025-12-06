"""
Seller Producer - Publishes stock availability to Kafka.
Acts as a stock seller offering stocks for sale.
"""
import sys
import uuid
from kafka import KafkaProducer
from kafka.errors import KafkaError
from models import SellOffer
from kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_SELL_OFFERS,
    serialize_message
)


class SellerProducer:
    """Seller producer for publishing stock availability"""

    def __init__(self, seller_name: str = "SELLER"):
        """
        Initialize Kafka producer.

        Args:
            seller_name: Name of the seller
        """
        self.seller_name = seller_name
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: v  # We handle serialization ourselves
            )
            print(f" {self.seller_name} Producer initialized and connected to Kafka")
        except KafkaError as e:
            print(f" Failed to initialize {self.seller_name} Producer: {e}")
            sys.exit(1)

    def send_sell_offer(self, stock_symbol: str, quantity: int):
        """
        Send a sell offer to Kafka.

        Args:
            stock_symbol: Stock symbol (e.g., 'SBI')
            quantity: Number of stocks available for sale
        """
        offer = SellOffer(
            offer_id=str(uuid.uuid4()),
            seller=self.seller_name,
            stock_symbol=stock_symbol,
            quantity=quantity
        )

        try:
            # Serialize and send to Kafka
            message = serialize_message(offer)
            future = self.producer.send(TOPIC_SELL_OFFERS, value=message)
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            print(f"\n{'='*60}")
            print(f" {self.seller_name}: Sell offer sent successfully!")
            print(f"{'='*60}")
            print(f"  Offer ID: {offer.offer_id}")
            print(f"  Stock: {offer.stock_symbol}")
            print(f"  Quantity available: {offer.quantity}")
            print(f"  Topic: {record_metadata.topic}")
            print(f"  Partition: {record_metadata.partition}")
            print(f"  Offset: {record_metadata.offset}")
            print(f"{'='*60}\n")
            
            return offer.offer_id

        except KafkaError as e:
            print(f" Failed to send sell offer: {e}")
            return None

    def close(self):
        """Close the producer connection"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            print(f" {self.seller_name} Producer connection closed")


def main():
    """Main function to demonstrate seller producer"""
    producer = SellerProducer(seller_name="SELLER")
    
    # Send sell offer for 100 SBI stocks
    producer.send_sell_offer(
        stock_symbol="SBI",
        quantity=1000
    )
    
    producer.close()


if __name__ == "__main__":
    main()
