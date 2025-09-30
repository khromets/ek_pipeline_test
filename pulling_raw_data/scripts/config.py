from datetime import datetime, timedelta
from pathlib import Path

class Config:
    """Configuration settings for data generation"""

    # Database settings
    DB_PATH = Path(__file__).parent.parent.parent / "data" / "raw" / "finance_data.db"

    # Data generation volumes
    DEFAULT_CUSTOMERS = 1000
    DEFAULT_ACCOUNTS_PER_CUSTOMER = 2
    DEFAULT_TRANSACTIONS_PER_ACCOUNT = 50

    # Date ranges
    CUSTOMER_JOIN_START = datetime.now() - timedelta(days=365*3)  # 3 years ago
    CUSTOMER_JOIN_END = datetime.now() - timedelta(days=30)       # 30 days ago

    TRANSACTION_START = datetime.now() - timedelta(days=365)      # 1 year ago
    TRANSACTION_END = datetime.now()                              # Now

    # Financial settings
    CURRENCIES = ["USD", "EUR", "GBP", "CAD"]
    ACCOUNT_TYPES = ["checking", "savings", "investment", "credit"]
    CUSTOMER_TYPES = ["individual", "business", "premium"]

    TRANSACTION_TYPES = ["deposit", "withdrawal", "transfer", "payment", "fee"]
    TRANSACTION_CATEGORIES = [
        "groceries", "restaurants", "gas", "shopping", "entertainment",
        "utilities", "rent", "insurance", "healthcare", "education",
        "travel", "investment", "salary", "bonus", "other"
    ]

    # Balance ranges
    MIN_ACCOUNT_BALANCE = 100.0
    MAX_ACCOUNT_BALANCE = 50000.0

    MIN_TRANSACTION_AMOUNT = 5.0
    MAX_TRANSACTION_AMOUNT = 5000.0

    # Backup settings
    BACKUP_JSON = True
    BACKUP_PATH = Path(__file__).parent.parent / "backup"

    # Logging
    LOG_PATH = Path(__file__).parent.parent.parent / "data" / "logs" / "generation_logs.txt"