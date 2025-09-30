from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import Optional
from decimal import Decimal

class Customer(BaseModel):
    customer_id: str
    name: str
    email: str
    phone: Optional[str] = None
    address: Optional[str] = None
    date_joined: date
    customer_type: str
    timestamp_insert: datetime

    class Config:
        json_encoders = {
            date: lambda v: v.isoformat(),
            datetime: lambda v: v.isoformat()
        }

class Account(BaseModel):
    account_id: str
    customer_id: str
    account_number: str
    account_type: str
    iban: str
    balance: Decimal = Field(decimal_places=2)
    currency: str
    created_date: date
    timestamp_insert: datetime

    class Config:
        json_encoders = {
            date: lambda v: v.isoformat(),
            datetime: lambda v: v.isoformat(),
            Decimal: lambda v: float(v)
        }

class Transaction(BaseModel):
    transaction_id: str
    account_id: str
    transaction_date: datetime
    transaction_type: str
    amount: Decimal = Field(decimal_places=2)
    currency: str
    description: str
    merchant: Optional[str] = None
    category: Optional[str] = None
    balance_after: Decimal = Field(decimal_places=2)
    timestamp_insert: datetime

    class Config:
        json_encoders = {
            date: lambda v: v.isoformat(),
            datetime: lambda v: v.isoformat(),
            Decimal: lambda v: float(v)
        }