"""
NSE Service - Acts as both consumer and producer.
Consumes buy orders and sell offers, matches them, and produces settlement messages.
"""
import sys
import uuid
import threading
from typing import Dict, List
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from models import BuyOrder, SellOffer, DematTransfer, CashMovement
from kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_BUY_ORDERS,
    TOPIC_SELL_OFFERS,
    TOPIC_DEMAT_TRANSFERS,
    TOPIC_CASH_MOVEMENTS,
    CONSUMER_GROUP_NSE,
    serialize_message,
    deserialize_message
)


class NSEService:
    """
    NSE Exchange Service - Hybrid Consumer/Producer
    Consumes buy orders and sell offers, matches them, and produces settlements
    """

    def __init__(self):
        """Initialize NSE service with consumers and producer"""
        self.buy_orders: Dict[str, BuyOrder] = {}
        self.sell_offers: Dict[str, SellOffer] = {}
        self.running = False
        
        try:
            # Initialize producer for settlements
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: v
            )
            
            # Initialize consumer for buy orders
            self.buy_consumer = KafkaConsumer(
                TOPIC_BUY_ORDERS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP_NSE,
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            # Initialize consumer for sell offers
            self.sell_consumer = KafkaConsumer(
                TOPIC_SELL_OFFERS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP_NSE,
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            print(f"✓ NSE Service initialized and connected to Kafka")
            
        except KafkaError as e:
            print(f"✗ Failed to initialize NSE Service: {e}")
            sys.exit(1)

    def process_buy_order(self, message):
        """Process incoming buy order"""
        try:
            order = deserialize_message(message.value, BuyOrder)
            self.buy_orders[order.order_id] = order
            
            print(f"\n{'='*60}")
            print(f"📥 NSE: Received buy order")
            print(f"{'='*60}")
            print(f"  Order ID: {order.order_id}")
            print(f"  Buyer: {order.buyer}")
            print(f"  Stock: {order.stock_symbol}")
            print(f"  Quantity: {order.quantity}")
            print(f"  Price: ₹{order.price_per_unit:,.2f}")
            print(f"  Total: ₹{order.total_amount:,.2f}")
            print(f"{'='*60}\n")
            
            # Try to match with available sell offers
            self.match_orders(order)
            
        except Exception as e:
            print(f"✗ Error processing buy order: {e}")

    def process_sell_offer(self, message):
        """Process incoming sell offer"""
        try:
            offer = deserialize_message(message.value, SellOffer)
            self.sell_offers[offer.offer_id] = offer
            
            print(f"\n{'='*60}")
            print(f"📥 NSE: Received sell offer")
            print(f"{'='*60}")
            print(f"  Offer ID: {offer.offer_id}")
            print(f"  Seller: {offer.seller}")
            print(f"  Stock: {offer.stock_symbol}")
            print(f"  Quantity: {offer.quantity}")
            print(f"{'='*60}\n")
            
            # Try to match with pending buy orders
            for order in self.buy_orders.values():
                if (order.stock_symbol == offer.stock_symbol and 
                    order.quantity <= offer.quantity):
                    self.match_orders(order)
                    break
                    
        except Exception as e:
            print(f"✗ Error processing sell offer: {e}")

    def match_orders(self, order: BuyOrder):
        """
        Match buy order with sell offers and execute settlement.
        
        Args:
            order: Buy order to match
        """
        # Find matching sell offer
        matching_offer = None
        for offer in self.sell_offers.values():
            if (offer.stock_symbol == order.stock_symbol and 
                offer.quantity >= order.quantity):
                matching_offer = offer
                break
        
        if matching_offer:
            print(f"\n{'='*60}")
            print(f"🎯 NSE: Order matched successfully!")
            print(f"{'='*60}")
            print(f"  Buy Order: {order.order_id}")
            print(f"  Sell Offer: {matching_offer.offer_id}")
            print(f"  Stock: {order.stock_symbol}")
            print(f"  Quantity: {order.quantity}")
            print(f"{'='*60}\n")
            
            # Execute settlement
            self.execute_settlement(order, matching_offer)
        else:
            print(f"⏳ NSE: Waiting for matching sell offer for order {order.order_id}")

    def execute_settlement(self, order: BuyOrder, offer: SellOffer):
        """
        Execute settlement by sending demat transfer and cash movement.
        
        Args:
            order: Matched buy order
            offer: Matched sell offer
        """
        # 1. Send demat transfer (stocks to buyer)
        demat_transfer = DematTransfer(
            transfer_id=str(uuid.uuid4()),
            from_entity="NSE",
            to_entity=order.buyer,
            stock_symbol=order.stock_symbol,
            quantity=order.quantity,
            order_id=order.order_id
        )
        
        try:
            message = serialize_message(demat_transfer)
            future = self.producer.send(TOPIC_DEMAT_TRANSFERS, value=message)
            future.get(timeout=10)
            
            print(f"\n{'='*60}")
            print(f"📤 NSE: Demat transfer sent")
            print(f"{'='*60}")
            print(f"  Transfer ID: {demat_transfer.transfer_id}")
            print(f"  From: {demat_transfer.from_entity}")
            print(f"  To: {demat_transfer.to_entity}")
            print(f"  Stock: {demat_transfer.stock_symbol}")
            print(f"  Quantity: {demat_transfer.quantity}")
            print(f"{'='*60}\n")
            
        except KafkaError as e:
            print(f"✗ Failed to send demat transfer: {e}")
            return

        # 2. Send cash movement (payment to buyer - negative flow from buyer perspective)
        cash_movement = CashMovement(
            movement_id=str(uuid.uuid4()),
            from_entity="NSE",
            to_entity=order.buyer,
            amount=order.total_amount,
            order_id=order.order_id,
            description=f"Payment for {order.quantity} {order.stock_symbol} stocks @ ₹{order.price_per_unit}"
        )
        
        try:
            message = serialize_message(cash_movement)
            future = self.producer.send(TOPIC_CASH_MOVEMENTS, value=message)
            future.get(timeout=10)
            
            print(f"\n{'='*60}")
            print(f"📤 NSE: Cash movement sent")
            print(f"{'='*60}")
            print(f"  Movement ID: {cash_movement.movement_id}")
            print(f"  From: {cash_movement.from_entity}")
            print(f"  To: {cash_movement.to_entity}")
            print(f"  Amount: ₹{cash_movement.amount:,.2f}")
            print(f"  Description: {cash_movement.description}")
            print(f"{'='*60}\n")
            
            print(f"✅ NSE: Settlement completed successfully!\n")
            
        except KafkaError as e:
            print(f"✗ Failed to send cash movement: {e}")

    def consume_buy_orders(self):
        """Consumer thread for buy orders"""
        print("🔄 NSE: Started consuming buy orders...")
        for message in self.buy_consumer:
            if not self.running:
                break
            self.process_buy_order(message)

    def consume_sell_offers(self):
        """Consumer thread for sell offers"""
        print("🔄 NSE: Started consuming sell offers...")
        for message in self.sell_consumer:
            if not self.running:
                break
            self.process_sell_offer(message)

    def start(self):
        """Start NSE service with multiple consumer threads"""
        self.running = True
        
        # Start consumer threads
        buy_thread = threading.Thread(target=self.consume_buy_orders, daemon=True)
        sell_thread = threading.Thread(target=self.consume_sell_offers, daemon=True)
        
        buy_thread.start()
        sell_thread.start()
        
        print("\n" + "="*60)
        print("🏛️  NSE Exchange Service Started")
        print("="*60)
        print("  Listening to:")
        print(f"    - {TOPIC_BUY_ORDERS}")
        print(f"    - {TOPIC_SELL_OFFERS}")
        print("  Publishing to:")
        print(f"    - {TOPIC_DEMAT_TRANSFERS}")
        print(f"    - {TOPIC_CASH_MOVEMENTS}")
        print("="*60 + "\n")
        
        return buy_thread, sell_thread

    def stop(self):
        """Stop NSE service"""
        self.running = False
        if self.producer:
            self.producer.flush()
            self.producer.close()
        if self.buy_consumer:
            self.buy_consumer.close()
        if self.sell_consumer:
            self.sell_consumer.close()
        print("\n✓ NSE Service stopped")


def main():
    """Main function to run NSE service"""
    service = NSEService()
    
    try:
        buy_thread, sell_thread = service.start()
        
        # Keep the service running
        buy_thread.join()
        sell_thread.join()
        
    except KeyboardInterrupt:
        print("\n⚠️  Shutting down NSE Service...")
        service.stop()


if __name__ == "__main__":
    main()
