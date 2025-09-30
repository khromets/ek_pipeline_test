import sqlite3
import logging
import json
from typing import List, Dict, Any
from pathlib import Path

class DatabaseOperationsEnhanced:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)

    # REPLACE MODE - Original behavior (replaces all data)
    def replace_customers(self, customers: List[Dict[str, Any]]):
        """Replace all customers in database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Clear existing data
            cursor.execute("DELETE FROM customers")

            # Insert new data
            customer_data = [
                (
                    customer['customer_id'],
                    customer['name'],
                    customer['email'],
                    customer['phone'],
                    customer['address'],
                    customer['date_joined'],
                    customer['customer_type'],
                    customer['timestamp_insert']
                )
                for customer in customers
            ]

            cursor.executemany("""
                INSERT INTO customers
                (customer_id, name, email, phone, address, date_joined, customer_type, timestamp_insert)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, customer_data)

            conn.commit()
            self.logger.info(f"Replaced with {len(customers)} customers")

    def replace_accounts(self, accounts: List[Dict[str, Any]]):
        """Replace all accounts in database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Clear existing data
            cursor.execute("DELETE FROM accounts")

            # Insert new data
            account_data = [
                (
                    account['account_id'],
                    account['customer_id'],
                    account['account_number'],
                    account['account_type'],
                    account['iban'],
                    float(account['balance']),
                    account['currency'],
                    account['created_date'],
                    account['timestamp_insert']
                )
                for account in accounts
            ]

            cursor.executemany("""
                INSERT INTO accounts
                (account_id, customer_id, account_number, account_type, iban, balance, currency, created_date, timestamp_insert)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, account_data)

            conn.commit()
            self.logger.info(f"Replaced with {len(accounts)} accounts")

    def replace_transactions(self, transactions: List[Dict[str, Any]]):
        """Replace all transactions in database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Clear existing data
            cursor.execute("DELETE FROM transactions")

            # Insert new data
            transaction_data = [
                (
                    transaction['transaction_id'],
                    transaction['account_id'],
                    transaction['transaction_date'],
                    transaction['transaction_type'],
                    float(transaction['amount']),
                    transaction['currency'],
                    transaction['description'],
                    transaction['merchant'],
                    transaction['category'],
                    float(transaction['balance_after']),
                    transaction['timestamp_insert']
                )
                for transaction in transactions
            ]

            cursor.executemany("""
                INSERT INTO transactions
                (transaction_id, account_id, transaction_date, transaction_type, amount,
                 currency, description, merchant, category, balance_after, timestamp_insert)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, transaction_data)

            conn.commit()
            self.logger.info(f"Replaced with {len(transactions)} transactions")

    # INSERT MODE - Add new records only (no updates)
    def insert_customers(self, customers: List[Dict[str, Any]]):
        """Insert new customers into database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            customer_data = [
                (
                    customer['customer_id'],
                    customer['name'],
                    customer['email'],
                    customer['phone'],
                    customer['address'],
                    customer['date_joined'],
                    customer['customer_type'],
                    customer['timestamp_insert']
                )
                for customer in customers
            ]

            cursor.executemany("""
                INSERT INTO customers
                (customer_id, name, email, phone, address, date_joined, customer_type, timestamp_insert)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, customer_data)

            conn.commit()
            self.logger.info(f"Inserted {len(customers)} new customers")

    def insert_accounts(self, accounts: List[Dict[str, Any]]):
        """Insert new accounts into database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            account_data = [
                (
                    account['account_id'],
                    account['customer_id'],
                    account['account_number'],
                    account['account_type'],
                    account['iban'],
                    float(account['balance']),
                    account['currency'],
                    account['created_date'],
                    account['timestamp_insert']
                )
                for account in accounts
            ]

            cursor.executemany("""
                INSERT INTO accounts
                (account_id, customer_id, account_number, account_type, iban, balance, currency, created_date, timestamp_insert)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, account_data)

            conn.commit()
            self.logger.info(f"Inserted {len(accounts)} new accounts")

    def insert_transactions(self, transactions: List[Dict[str, Any]]):
        """Insert new transactions into database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            transaction_data = [
                (
                    transaction['transaction_id'],
                    transaction['account_id'],
                    transaction['transaction_date'],
                    transaction['transaction_type'],
                    float(transaction['amount']),
                    transaction['currency'],
                    transaction['description'],
                    transaction['merchant'],
                    transaction['category'],
                    float(transaction['balance_after']),
                    transaction['timestamp_insert']
                )
                for transaction in transactions
            ]

            cursor.executemany("""
                INSERT INTO transactions
                (transaction_id, account_id, transaction_date, transaction_type, amount,
                 currency, description, merchant, category, balance_after, timestamp_insert)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, transaction_data)

            conn.commit()
            self.logger.info(f"Inserted {len(transactions)} new transactions")

    # MERGE MODE - Insert new, update existing (upsert)
    def merge_customers(self, customers: List[Dict[str, Any]]):
        """Merge customers into database (insert new, update existing)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            customer_data = [
                (
                    customer['customer_id'],
                    customer['name'],
                    customer['email'],
                    customer['phone'],
                    customer['address'],
                    customer['date_joined'],
                    customer['customer_type'],
                    customer['timestamp_insert']
                )
                for customer in customers
            ]

            cursor.executemany("""
                INSERT OR REPLACE INTO customers
                (customer_id, name, email, phone, address, date_joined, customer_type, timestamp_insert)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, customer_data)

            conn.commit()
            self.logger.info(f"Merged {len(customers)} customers")

    def merge_accounts(self, accounts: List[Dict[str, Any]]):
        """Merge accounts into database (insert new, update existing)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            account_data = [
                (
                    account['account_id'],
                    account['customer_id'],
                    account['account_number'],
                    account['account_type'],
                    account['iban'],
                    float(account['balance']),
                    account['currency'],
                    account['created_date'],
                    account['timestamp_insert']
                )
                for account in accounts
            ]

            cursor.executemany("""
                INSERT OR REPLACE INTO accounts
                (account_id, customer_id, account_number, account_type, iban, balance, currency, created_date, timestamp_insert)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, account_data)

            conn.commit()
            self.logger.info(f"Merged {len(accounts)} accounts")

    def merge_transactions(self, transactions: List[Dict[str, Any]]):
        """Merge transactions into database (insert new, update existing)"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            transaction_data = [
                (
                    transaction['transaction_id'],
                    transaction['account_id'],
                    transaction['transaction_date'],
                    transaction['transaction_type'],
                    float(transaction['amount']),
                    transaction['currency'],
                    transaction['description'],
                    transaction['merchant'],
                    transaction['category'],
                    float(transaction['balance_after']),
                    transaction['timestamp_insert']
                )
                for transaction in transactions
            ]

            cursor.executemany("""
                INSERT OR REPLACE INTO transactions
                (transaction_id, account_id, transaction_date, transaction_type, amount,
                 currency, description, merchant, category, balance_after, timestamp_insert)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, transaction_data)

            conn.commit()
            self.logger.info(f"Merged {len(transactions)} transactions")

    # UTILITY METHODS
    def get_stats(self):
        """Get database statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            stats = {}
            for table in ['customers', 'accounts', 'transactions']:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                stats[table] = cursor.fetchone()[0]

            return stats

    def get_stats_by_timestamp(self):
        """Get database statistics grouped by timestamp_insert"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            stats = {}
            for table in ['customers', 'accounts', 'transactions']:
                cursor.execute(f"""
                    SELECT DATE(timestamp_insert) as load_date, COUNT(*) as count
                    FROM {table}
                    GROUP BY DATE(timestamp_insert)
                    ORDER BY load_date DESC
                """)
                stats[table] = cursor.fetchall()

            return stats

    def backup_to_json(self, backup_path: Path):
        """Export database to JSON files"""
        backup_path.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            for table in ['customers', 'accounts', 'transactions']:
                cursor = conn.cursor()
                cursor.execute(f"SELECT * FROM {table}")
                rows = [dict(row) for row in cursor.fetchall()]

                with open(backup_path / f"{table}.json", 'w') as f:
                    json.dump(rows, f, indent=2, default=str)

                self.logger.info(f"Backed up {len(rows)} {table} to JSON")

    def show_load_history(self):
        """Show loading history by timestamp"""
        print("\n" + "="*60)
        print("DATA LOADING HISTORY")
        print("="*60)

        stats = self.get_stats_by_timestamp()
        for table, data in stats.items():
            print(f"\n{table.upper()}:")
            for load_date, count in data:
                print(f"  {load_date}: {count:,} records")

        print("="*60)