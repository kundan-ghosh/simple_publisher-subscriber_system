"""
HDFC Consumer - Receives demat transfers and cash movements from NSE.
Acts as the buyer receiving stock transfers and cash settlement confirmations.
"""
import sys
import threading
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from models import DematTransfer, CashMovement
from kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_DEMAT_TRANSFERS,
    TOPIC_CASH_MOVEMENTS,
    CONSUMER_GROUP_HDFC,
    deserialize_message
)


class HDFCConsumer:
    """HDFC consumer for receiving settlement messages"""

    def __init__(self):
        """Initialize Kafka consumers"""
        self.running = False
        
        try:
            # Initialize consumer for demat transfers
            self.demat_consumer = KafkaConsumer(
                TOPIC_DEMAT_TRANSFERS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP_HDFC,
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            # Initialize consumer for cash movements
            self.cash_consumer = KafkaConsumer(
                TOPIC_CASH_MOVEMENTS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP_HDFC,
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            
            print(f"✓ HDFC Consumer initialized and connected to Kafka")
            
        except KafkaError as e:
            print(f"✗ Failed to initialize HDFC Consumer: {e}")
            sys.exit(1)

    def process_demat_transfer(self, message):
        """Process incoming demat transfer"""
        try:
            transfer = deserialize_message(message.value, DematTransfer)
            
            print(f"\n{'='*60}")
            print(f"📥 HDFC: Demat transfer received!")
            print(f"{'='*60}")
            print(f"  Transfer ID: {transfer.transfer_id}")
            print(f"  From: {transfer.from_entity}")
            print(f"  To: {transfer.to_entity}")
            print(f"  Stock: {transfer.stock_symbol}")
            print(f"  Quantity: {transfer.quantity}")
            print(f"  Order ID: {transfer.order_id}")
            print(f"  ✅ {transfer.quantity} {transfer.stock_symbol} stocks credited to demat account")
            print(f"{'='*60}\n")
            
        except Exception as e:
            print(f"✗ Error processing demat transfer: {e}")

    def process_cash_movement(self, message):
        """Process incoming cash movement"""
        try:
            movement = deserialize_message(message.value, CashMovement)
            
            print(f"\n{'='*60}")
            print(f"📥 HDFC: Cash movement received!")
            print(f"{'='*60}")
            print(f"  Movement ID: {movement.movement_id}")
            print(f"  From: {movement.from_entity}")
            print(f"  To: {movement.to_entity}")
            print(f"  Amount: ₹{movement.amount:,.2f}")
            print(f"  Order ID: {movement.order_id}")
            print(f"  Description: {movement.description}")
            print(f"  💰 ₹{movement.amount:,.2f} debited from account")
            print(f"{'='*60}\n")
            
        except Exception as e:
            print(f"✗ Error processing cash movement: {e}")

    def consume_demat_transfers(self):
        """Consumer thread for demat transfers"""
        print("🔄 HDFC: Started consuming demat transfers...")
        for message in self.demat_consumer:
            if not self.running:
                break
            self.process_demat_transfer(message)

    def consume_cash_movements(self):
        """Consumer thread for cash movements"""
        print("🔄 HDFC: Started consuming cash movements...")
        for message in self.cash_consumer:
            if not self.running:
                break
            self.process_cash_movement(message)

    def start(self):
        """Start HDFC consumer with multiple consumer threads"""
        self.running = True
        
        # Start consumer threads
        demat_thread = threading.Thread(target=self.consume_demat_transfers, daemon=True)
        cash_thread = threading.Thread(target=self.consume_cash_movements, daemon=True)
        
        demat_thread.start()
        cash_thread.start()
        
        print("\n" + "="*60)
        print("🏦 HDFC Consumer Service Started")
        print("="*60)
        print("  Listening to:")
        print(f"    - {TOPIC_DEMAT_TRANSFERS}")
        print(f"    - {TOPIC_CASH_MOVEMENTS}")
        print("="*60 + "\n")
        
        return demat_thread, cash_thread

    def stop(self):
        """Stop HDFC consumer"""
        self.running = False
        if self.demat_consumer:
            self.demat_consumer.close()
        if self.cash_consumer:
            self.cash_consumer.close()
        print("\n✓ HDFC Consumer stopped")


def main():
    """Main function to run HDFC consumer"""
    consumer = HDFCConsumer()
    
    try:
        demat_thread, cash_thread = consumer.start()
        
        # Keep the consumer running
        demat_thread.join()
        cash_thread.join()
        
    except KeyboardInterrupt:
        print("\n⚠️  Shutting down HDFC Consumer...")
        consumer.stop()


if __name__ == "__main__":
    main()
