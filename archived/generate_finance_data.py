#!/usr/bin/env python3

import argparse
import logging
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any
from pathlib import Path
import sys
import json

from faker import Faker

# Add parent directories to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from data_models import Customer, Account, Transaction
from config import Config
from data.scripts.database_setup import DatabaseSetup
from data.scripts.db_operations import DatabaseOperations

class FinanceDataGenerator:
    def __init__(self):
        self.fake = Faker()
        self.config = Config()
        self.logger = logging.getLogger(__name__)

    def generate_customers(self, count: int) -> List[Dict[str, Any]]:
        """Generate synthetic customer data"""
        customers = []
        insert_timestamp = datetime(2025, 9, 18, 18, 0, 0)  # 2025-09-18 1PM ET (6PM UTC)

        for _ in range(count):
            customer = Customer(
                customer_id=str(uuid.uuid4()),
                name=self.fake.name(),
                email=self.fake.email(),
                phone=self.fake.phone_number(),
                address=self.fake.address().replace('\n', ', '),
                date_joined=self.fake.date_between(
                    start_date=self.config.CUSTOMER_JOIN_START,
                    end_date=self.config.CUSTOMER_JOIN_END
                ),
                customer_type=random.choice(self.config.CUSTOMER_TYPES),
                timestamp_insert=insert_timestamp
            )
            customers.append(customer.model_dump())

        self.logger.info(f"Generated {len(customers)} customers")
        return customers

    def generate_accounts(self, customers: List[Dict[str, Any]], accounts_per_customer: int) -> List[Dict[str, Any]]:
        """Generate synthetic account data for customers"""
        accounts = []
        insert_timestamp = datetime(2025, 9, 18, 18, 0, 0)  # 2025-09-18 1PM ET (6PM UTC)

        for customer in customers:
            # Handle date conversion properly
            if isinstance(customer['date_joined'], str):
                customer_join_date = datetime.fromisoformat(customer['date_joined']).date()
            else:
                customer_join_date = customer['date_joined']

            for _ in range(accounts_per_customer):
                # Account created after customer joined
                created_date = self.fake.date_between(
                    start_date=customer_join_date,
                    end_date=datetime.now().date()
                )

                account = Account(
                    account_id=str(uuid.uuid4()),
                    customer_id=customer['customer_id'],
                    account_number=self.fake.bban(),
                    account_type=random.choice(self.config.ACCOUNT_TYPES),
                    iban=self.fake.iban(),
                    balance=Decimal(str(round(random.uniform(
                        self.config.MIN_ACCOUNT_BALANCE,
                        self.config.MAX_ACCOUNT_BALANCE
                    ), 2))),
                    currency=random.choice(self.config.CURRENCIES),
                    created_date=created_date,
                    timestamp_insert=insert_timestamp
                )
                accounts.append(account.model_dump())

        self.logger.info(f"Generated {len(accounts)} accounts")
        return accounts

    def generate_transactions(self, accounts: List[Dict[str, Any]], transactions_per_account: int) -> List[Dict[str, Any]]:
        """Generate synthetic transaction data for accounts"""
        transactions = []
        insert_timestamp = datetime(2025, 9, 18, 18, 0, 0)  # 2025-09-18 1PM ET (6PM UTC)

        for account in accounts:
            # Handle date conversion properly
            if isinstance(account['created_date'], str):
                account_created = datetime.fromisoformat(account['created_date']).date()
            else:
                account_created = account['created_date']
            current_balance = Decimal(str(account['balance']))

            # Generate transactions in chronological order
            transaction_dates = []
            for _ in range(transactions_per_account):
                transaction_date = self.fake.date_time_between(
                    start_date=max(account_created, self.config.TRANSACTION_START.date()),
                    end_date=self.config.TRANSACTION_END
                )
                transaction_dates.append(transaction_date)

            transaction_dates.sort()

            for transaction_date in transaction_dates:
                transaction_type = random.choice(self.config.TRANSACTION_TYPES)

                # Determine transaction amount and direction
                amount = Decimal(str(round(random.uniform(
                    self.config.MIN_TRANSACTION_AMOUNT,
                    self.config.MAX_TRANSACTION_AMOUNT
                ), 2)))

                # Apply transaction to balance
                if transaction_type in ['deposit', 'salary', 'bonus']:
                    current_balance += amount
                elif transaction_type in ['withdrawal', 'payment', 'fee']:
                    current_balance -= amount
                    # Ensure balance doesn't go too negative
                    if current_balance < -1000:
                        current_balance += amount  # Reverse transaction
                        amount = Decimal('0.01')  # Minimal fee instead

                # Generate merchant for certain transaction types
                merchant = None
                if transaction_type in ['payment', 'withdrawal']:
                    merchant = self.fake.company()

                # Generate category
                category = random.choice(self.config.TRANSACTION_CATEGORIES)
                if transaction_type in ['deposit', 'salary', 'bonus']:
                    category = 'income'

                transaction = Transaction(
                    transaction_id=str(uuid.uuid4()),
                    account_id=account['account_id'],
                    transaction_date=transaction_date,
                    transaction_type=transaction_type,
                    amount=amount,
                    currency=account['currency'],
                    description=self.fake.bs(),
                    merchant=merchant,
                    category=category,
                    balance_after=current_balance,
                    timestamp_insert=insert_timestamp
                )
                transactions.append(transaction.model_dump())

        self.logger.info(f"Generated {len(transactions)} transactions")
        return transactions

    def run_generation(self, customers_count: int, accounts_per_customer: int, transactions_per_account: int):
        """Main method to run the complete data generation pipeline"""
        self.logger.info("Starting finance data generation pipeline")

        # Step 1: Setup database
        db_setup = DatabaseSetup(str(self.config.DB_PATH))
        db_setup.create_database()

        # Step 2: Generate data
        customers = self.generate_customers(customers_count)
        accounts = self.generate_accounts(customers, accounts_per_customer)
        transactions = self.generate_transactions(accounts, transactions_per_account)

        # Step 3: Store in database
        db_ops = DatabaseOperations(str(self.config.DB_PATH))
        db_ops.batch_insert_customers(customers)
        db_ops.batch_insert_accounts(accounts)
        db_ops.batch_insert_transactions(transactions)

        # Step 4: Create backup if configured
        if self.config.BACKUP_JSON:
            self.config.BACKUP_PATH.mkdir(parents=True, exist_ok=True)
            db_ops.backup_to_json(self.config.BACKUP_PATH)

        # Step 5: Report statistics
        stats = db_ops.get_stats()
        self.logger.info("Data generation completed successfully!")
        self.logger.info(f"Database statistics: {stats}")

        return stats

def setup_logging():
    """Setup logging configuration"""
    Config.LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(Config.LOG_PATH),
            logging.StreamHandler()
        ]
    )

def main():
    parser = argparse.ArgumentParser(description='Generate synthetic finance data')
    parser.add_argument('--customers', type=int, default=Config.DEFAULT_CUSTOMERS,
                        help=f'Number of customers to generate (default: {Config.DEFAULT_CUSTOMERS})')
    parser.add_argument('--accounts-per-customer', type=int, default=Config.DEFAULT_ACCOUNTS_PER_CUSTOMER,
                        help=f'Accounts per customer (default: {Config.DEFAULT_ACCOUNTS_PER_CUSTOMER})')
    parser.add_argument('--transactions-per-account', type=int, default=Config.DEFAULT_TRANSACTIONS_PER_ACCOUNT,
                        help=f'Transactions per account (default: {Config.DEFAULT_TRANSACTIONS_PER_ACCOUNT})')

    args = parser.parse_args()

    setup_logging()

    generator = FinanceDataGenerator()
    stats = generator.run_generation(
        args.customers,
        args.accounts_per_customer,
        args.transactions_per_account
    )

    print("\n" + "="*50)
    print("FINANCE DATA GENERATION COMPLETE")
    print("="*50)
    for table, count in stats.items():
        print(f"{table.upper()}: {count:,} records")
    print("="*50)

if __name__ == "__main__":
    main()