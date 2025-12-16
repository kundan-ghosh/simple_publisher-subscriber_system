"""
Data models for stock trading messages using Pydantic.
"""
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


class BuyOrder(BaseModel):
    """Buy order from a buyer (e.g., HDFC)"""
    order_id: str = Field(..., description="Unique order identifier")
    buyer: str = Field(..., description="Buyer name (e.g., HDFC)")
    stock_symbol: str = Field(..., description="Stock symbol (e.g., SBI)")
    quantity: int = Field(..., gt=0, description="Number of stocks to buy")
    price_per_unit: float = Field(..., gt=0, description="Price per stock unit")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

    @property
    def total_amount(self) -> float:
        """Calculate total order amount"""
        return self.quantity * self.price_per_unit


class SellOffer(BaseModel):
    """Sell offer from a seller"""
    offer_id: str = Field(..., description="Unique offer identifier")
    seller: str = Field(..., description="Seller name")
    stock_symbol: str = Field(..., description="Stock symbol (e.g., SBI)")
    quantity: int = Field(..., gt=0, description="Number of stocks available")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class DematTransfer(BaseModel):
    """Demat stock transfer from NSE to buyer"""
    transfer_id: str = Field(..., description="Unique transfer identifier")
    from_entity: str = Field(default="NSE", description="Source entity")
    to_entity: str = Field(..., description="Destination entity (e.g., HDFC)")
    stock_symbol: str = Field(..., description="Stock symbol")
    quantity: int = Field(..., gt=0, description="Number of stocks transferred")
    order_id: str = Field(..., description="Original order ID")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class CashMovement(BaseModel):
    """Cash settlement from NSE"""
    movement_id: str = Field(..., description="Unique movement identifier")
    from_entity: str = Field(default="NSE", description="Source entity")
    to_entity: str = Field(..., description="Destination entity (e.g., HDFC)")
    amount: float = Field(..., gt=0, description="Cash amount")
    order_id: str = Field(..., description="Related order ID")
    description: str = Field(..., description="Movement description")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
