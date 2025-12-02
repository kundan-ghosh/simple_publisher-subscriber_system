# Stock Trading Pub-Sub System with Kafka

A multi-party publisher-subscriber system using **Python** and **Apache Kafka** to simulate stock trading between HDFC (buyer), a Seller (stock provider), and NSE (National Stock Exchange).

##  Overview

This project demonstrates a real-world pub-sub architecture where:
1. **HDFC** (buyer) sends a buy order for 100 SBI stocks at ₹885
2. **Seller** publishes stock availability 
3. **NSE** (exchange) matches orders and executes settlement
4. **NSE** transfers stocks (demat) to HDFC
5. **NSE** processes cash movement (₹88,500) for the transaction

##  Architecture

```
┌──────────────┐      buy-orders        ┌──────────────┐
│     HDFC     │──────────────────────▶│              │
│  (Producer)  │                        │              │
└──────────────┘                        │              │
                                        │     NSE      │
┌──────────────┐     sell-offers       │  (Consumer   │
│    Seller    │──────────────────────▶│      &       │
│  (Producer)  │                        │   Producer)  │
└──────────────┘                        │              │
                                        └───────┬──────┘
                                                │
                        ┌───────────────────────┴────────────┐
                        │                                    │
                        ▼                                    ▼
                demat-transfers                      cash-movements
                        │                                    │
                        │         ┌──────────────┐          │
                        └────────▶│     HDFC     │◀─────────┘
                                  │  (Consumer)  │
                                  └──────────────┘
```

##  Prerequisites

- **Python 3.7+**
- **pip** (Python package manager)
- **Homebrew** (for installing Kafka on Mac)

##  Quick Start

### 1. Navigate to Project Directory

```bash
cd ~/kafka-stock-trading
```

### 2. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 3. Start Kafka Locally

## Kafka 4.x runs in KRaft mode (no ZooKeeper)

# This uses the kafka-storage tool that’s designed for KRaft to generate a cluster UUID.

# Format the log directory using that ID
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
echo "$KAFKA_CLUSTER_ID"

# The default config/server.properties already has:
# * process.roles=broker,controller
# * node.id=1
# * controller.quorum.voters=1@localhost:9093

bin/kafka-storage.sh format \
  -t "$KAFKA_CLUSTER_ID" \
  -c config/server.properties --standalone

# Starting the broker + controller. Leave this terminal open; it’s your broker+controller.
bin/kafka-server-start.sh config/server.properties



##  Project Structure

```
kafka-stock-trading/
├── requirements.txt            # Python dependencies
├── models.py                   # Pydantic data models
├── kafka_config.py             # Kafka configuration & utilities
├── hdfc_producer.py            # HDFC buy order producer
├── seller_producer.py          # Seller stock availability producer
├── nse_service.py              # NSE exchange service (consumer + producer)
├── hdfc_consumer.py            # HDFC settlement consumer
└── README.md                   # This file
```

##  Components

### Data Models (`models.py`)

- **BuyOrder**: Order from buyer (HDFC)
- **SellOffer**: Stock availability from seller
- **DematTransfer**: Stock transfer confirmation from NSE
- **CashMovement**: Cash settlement details from NSE

### Kafka Topics

| Topic | Producer | Consumer | Purpose |
|-------|----------|----------|---------|
| `buy-orders` | HDFC | NSE | Buy order requests |
| `sell-offers` | Seller | NSE | Stock availability |
| `demat-transfers` | NSE | HDFC | Stock transfers |
| `cash-movements` | NSE | HDFC | Cash settlements |

### Services

#### HDFC Producer (`hdfc_producer.py`)
- Publishes buy orders to `buy-orders` topic
- Example: 100 SBI stocks @ ₹885

#### Seller Producer (`seller_producer.py`)
- Publishes stock availability to `sell-offers` topic
- Example: 100 SBI stocks available

#### NSE Service (`nse_service.py`)
- **Consumes**: `buy-orders` and `sell-offers`
- **Matches**: Orders with available stocks
- **Produces**: `demat-transfers` and `cash-movements`

#### HDFC Consumer (`hdfc_consumer.py`)
- **Consumes**: `demat-transfers` and `cash-movements`
- Displays received stock transfers and cash settlements

##  Running Individual Components

You can also run components individually in separate terminals:

### Terminal 1: Start NSE Service
```bash
run kafka server
```
### Terminal 2: Start NSE Service
```bash
python3 nse_service.py
```

### Terminal 3: Start HDFC Consumer
```bash
python3 hdfc_consumer.py
```

### Terminal 4: Send Buy Order
```bash
python3 hdfc_producer.py
```

### Terminal 5: Send Sell Offer
```bash
python3 seller_producer.py
```


##  Message Flow Example

```
Step 1: HDFC → NSE (buy-orders topic)
{
  "order_id": "uuid",
  "buyer": "HDFC",
  "stock_symbol": "SBI",
  "quantity": 100,
  "price_per_unit": 885.0
}

Step 2: Seller → NSE (sell-offers topic)
{
  "offer_id": "uuid",
  "seller": "SELLER",
  "stock_symbol": "SBI",
  "quantity": 100
}

Step 3: NSE → HDFC (demat-transfers topic)
{
  "transfer_id": "uuid",
  "from_entity": "NSE",
  "to_entity": "HDFC",
  "stock_symbol": "SBI",
  "quantity": 100,
  "order_id": "original-order-uuid"
}

Step 4: NSE → HDFC (cash-movements topic)
{
  "movement_id": "uuid",
  "from_entity": "NSE",
  "to_entity": "HDFC",
  "amount": 88500.0,
  "order_id": "original-order-uuid",
  "description": "Payment for 100 SBI stocks @ ₹885"
}
```


##  License

This project is for educational purposes.

---

**Happy Coding!**
