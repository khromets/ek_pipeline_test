#!/usr/bin/env python3

import argparse
import logging
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Set
from pathlib import Path
import sys
import json
import sqlite3

from faker import Faker

# Add parent directories to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from data_models import Customer, Account, Transaction
from config import Config
from data.scripts.database_setup import DatabaseSetup
from data.scripts.db_operations_enhanced import DatabaseOperationsEnhanced

class FinanceDataGeneratorEnhanced:
    def __init__(self, load_mode: str = "replace"):
        self.fake = Faker()
        self.config = Config()
        self.logger = logging.getLogger(__name__)
        self.load_mode = load_mode
        self.existing_customer_ids: Set[str] = set()
        self.existing_account_ids: Set[str] = set()
        self.existing_transaction_ids: Set[str] = set()

    def load_existing_ids(self):
        """Load existing IDs from database to avoid conflicts"""
        if self.load_mode in ["insert", "merge"]:
            try:
                with sqlite3.connect(str(self.config.DB_PATH)) as conn:
                    cursor = conn.cursor()

                    # Load existing customer IDs
                    cursor.execute("SELECT customer_id FROM customers")
                    self.existing_customer_ids = {row[0] for row in cursor.fetchall()}

                    # Load existing account IDs
                    cursor.execute("SELECT account_id FROM accounts")
                    self.existing_account_ids = {row[0] for row in cursor.fetchall()}

                    # Load existing transaction IDs
                    cursor.execute("SELECT transaction_id FROM transactions")
                    self.existing_transaction_ids = {row[0] for row in cursor.fetchall()}

                    self.logger.info(f"Loaded {len(self.existing_customer_ids)} existing customer IDs")
                    self.logger.info(f"Loaded {len(self.existing_account_ids)} existing account IDs")
                    self.logger.info(f"Loaded {len(self.existing_transaction_ids)} existing transaction IDs")

            except sqlite3.OperationalError:
                # Database doesn't exist or tables don't exist
                self.logger.info("No existing database found, starting fresh")

    def generate_unique_id(self, existing_ids: Set[str]) -> str:
        """Generate a unique UUID that doesn't conflict with existing IDs"""
        while True:
            new_id = str(uuid.uuid4())
            if new_id not in existing_ids:
                existing_ids.add(new_id)  # Add to set to avoid future conflicts
                return new_id

    def get_existing_customers(self) -> List[Dict[str, Any]]:
        """Get existing customers from database for merge mode"""
        customers = []
        if self.load_mode == "merge":
            try:
                with sqlite3.connect(str(self.config.DB_PATH)) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute("SELECT * FROM customers")
                    customers = [dict(row) for row in cursor.fetchall()]
                    self.logger.info(f"Loaded {len(customers)} existing customers for merge")
            except sqlite3.OperationalError:
                self.logger.info("No existing customers table found")
        return customers

    def generate_customers(self, count: int) -> List[Dict[str, Any]]:
        """Generate synthetic customer data with conflict resolution"""
        customers = []
        insert_timestamp = datetime.now()  # Current timestamp for new loads

        # For merge mode, start with existing customers
        if self.load_mode == "merge":
            existing_customers = self.get_existing_customers()
            customers.extend(existing_customers)
            self.logger.info(f"Starting with {len(existing_customers)} existing customers")

        # Generate new customers
        new_customers_needed = count - len(customers) if self.load_mode == "merge" else count

        if new_customers_needed > 0:
            for _ in range(new_customers_needed):
                customer_id = (self.generate_unique_id(self.existing_customer_ids)
                             if self.load_mode in ["insert", "merge"]
                             else str(uuid.uuid4()))

                customer = Customer(
                    customer_id=customer_id,
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

        self.logger.info(f"Generated {len(customers)} total customers ({new_customers_needed} new)")
        return customers

    def generate_accounts(self, customers: List[Dict[str, Any]], accounts_per_customer: int) -> List[Dict[str, Any]]:
        """Generate synthetic account data for customers with conflict resolution"""
        accounts = []
        insert_timestamp = datetime.now()

        # For merge mode, load existing accounts
        if self.load_mode == "merge":
            try:
                with sqlite3.connect(str(self.config.DB_PATH)) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute("SELECT * FROM accounts")
                    existing_accounts = [dict(row) for row in cursor.fetchall()]
                    accounts.extend(existing_accounts)
                    self.logger.info(f"Starting with {len(existing_accounts)} existing accounts")
            except sqlite3.OperationalError:
                pass

        # Generate new accounts for customers
        existing_customer_accounts = {}
        if self.load_mode == "merge":
            # Count existing accounts per customer
            for account in accounts:
                cust_id = account['customer_id']
                existing_customer_accounts[cust_id] = existing_customer_accounts.get(cust_id, 0) + 1

        for customer in customers:
            customer_id = customer['customer_id']
            existing_count = existing_customer_accounts.get(customer_id, 0)
            accounts_needed = max(0, accounts_per_customer - existing_count)

            # Handle date conversion properly
            if isinstance(customer['date_joined'], str):
                customer_join_date = datetime.fromisoformat(customer['date_joined']).date()
            else:
                customer_join_date = customer['date_joined']

            for _ in range(accounts_needed):
                # Account created after customer joined
                created_date = self.fake.date_between(
                    start_date=customer_join_date,
                    end_date=datetime.now().date()
                )

                account_id = (self.generate_unique_id(self.existing_account_ids)
                            if self.load_mode in ["insert", "merge"]
                            else str(uuid.uuid4()))

                account = Account(
                    account_id=account_id,
                    customer_id=customer_id,
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

        self.logger.info(f"Generated {len(accounts)} total accounts")
        return accounts

    def generate_transactions(self, accounts: List[Dict[str, Any]], transactions_per_account: int) -> List[Dict[str, Any]]:
        """Generate synthetic transaction data for accounts with conflict resolution"""
        transactions = []
        insert_timestamp = datetime.now()

        # For merge mode, load existing transactions
        if self.load_mode == "merge":
            try:
                with sqlite3.connect(str(self.config.DB_PATH)) as conn:
                    conn.row_factory = sqlite3.Row
                    cursor = conn.cursor()
                    cursor.execute("SELECT * FROM transactions")
                    existing_transactions = [dict(row) for row in cursor.fetchall()]
                    transactions.extend(existing_transactions)
                    self.logger.info(f"Starting with {len(existing_transactions)} existing transactions")
            except sqlite3.OperationalError:
                pass

        # Count existing transactions per account for merge mode
        existing_account_transactions = {}
        if self.load_mode == "merge":
            for transaction in transactions:
                acc_id = transaction['account_id']
                existing_account_transactions[acc_id] = existing_account_transactions.get(acc_id, 0) + 1

        for account in accounts:
            account_id = account['account_id']
            existing_count = existing_account_transactions.get(account_id, 0)
            transactions_needed = max(0, transactions_per_account - existing_count)

            if transactions_needed == 0:
                continue

            # Handle date conversion properly
            if isinstance(account['created_date'], str):
                account_created = datetime.fromisoformat(account['created_date']).date()
            else:
                account_created = account['created_date']

            current_balance = Decimal(str(account['balance']))

            # Generate transactions in chronological order
            transaction_dates = []
            for _ in range(transactions_needed):
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

                transaction_id = (self.generate_unique_id(self.existing_transaction_ids)
                                if self.load_mode in ["insert", "merge"]
                                else str(uuid.uuid4()))

                transaction = Transaction(
                    transaction_id=transaction_id,
                    account_id=account_id,
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

        self.logger.info(f"Generated {len(transactions)} total transactions")
        return transactions

    def run_generation(self, customers_count: int, accounts_per_customer: int, transactions_per_account: int):
        """Main method to run the complete data generation pipeline"""
        self.logger.info(f"Starting finance data generation pipeline - Mode: {self.load_mode.upper()}")

        # Step 1: Setup database
        db_setup = DatabaseSetup(str(self.config.DB_PATH))
        db_setup.create_database()

        # Step 2: Load existing IDs if needed
        self.load_existing_ids()

        # Step 3: Generate data
        customers = self.generate_customers(customers_count)
        accounts = self.generate_accounts(customers, accounts_per_customer)
        transactions = self.generate_transactions(accounts, transactions_per_account)

        # Step 4: Store in database
        db_ops = DatabaseOperationsEnhanced(str(self.config.DB_PATH))

        if self.load_mode == "replace":
            db_ops.replace_customers(customers)
            db_ops.replace_accounts(accounts)
            db_ops.replace_transactions(transactions)
        elif self.load_mode == "insert":
            db_ops.insert_customers(customers)
            db_ops.insert_accounts(accounts)
            db_ops.insert_transactions(transactions)
        elif self.load_mode == "merge":
            db_ops.merge_customers(customers)
            db_ops.merge_accounts(accounts)
            db_ops.merge_transactions(transactions)

        # Step 5: Create backup if configured
        if self.config.BACKUP_JSON:
            self.config.BACKUP_PATH.mkdir(parents=True, exist_ok=True)
            db_ops.backup_to_json(self.config.BACKUP_PATH)

        # Step 6: Report statistics
        stats = db_ops.get_stats()
        self.logger.info(f"Data generation completed successfully! Mode: {self.load_mode.upper()}")
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
    parser = argparse.ArgumentParser(description='Generate synthetic finance data with flexible loading modes')
    parser.add_argument('--customers', type=int, default=Config.DEFAULT_CUSTOMERS,
                        help=f'Number of customers to generate (default: {Config.DEFAULT_CUSTOMERS})')
    parser.add_argument('--accounts-per-customer', type=int, default=Config.DEFAULT_ACCOUNTS_PER_CUSTOMER,
                        help=f'Accounts per customer (default: {Config.DEFAULT_ACCOUNTS_PER_CUSTOMER})')
    parser.add_argument('--transactions-per-account', type=int, default=Config.DEFAULT_TRANSACTIONS_PER_ACCOUNT,
                        help=f'Transactions per account (default: {Config.DEFAULT_TRANSACTIONS_PER_ACCOUNT})')
    parser.add_argument('--mode', type=str, choices=['replace', 'insert', 'merge'], default='replace',
                        help='Loading mode: replace (default), insert (add new), merge (upsert)')

    args = parser.parse_args()

    setup_logging()

    generator = FinanceDataGeneratorEnhanced(load_mode=args.mode)
    stats = generator.run_generation(
        args.customers,
        args.accounts_per_customer,
        args.transactions_per_account
    )

    print("\n" + "="*60)
    print(f"FINANCE DATA GENERATION COMPLETE - MODE: {args.mode.upper()}")
    print("="*60)
    for table, count in stats.items():
        print(f"{table.upper()}: {count:,} records")
    print("="*60)

    # Show mode explanation
    if args.mode == "replace":
        print("MODE: REPLACE - All existing data was replaced with new data")
    elif args.mode == "insert":
        print("MODE: INSERT - New data was added to existing data")
    elif args.mode == "merge":
        print("MODE: MERGE - Existing data kept, new records added as needed")

if __name__ == "__main__":
    main()