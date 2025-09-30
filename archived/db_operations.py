import sqlite3
import logging
import json
from typing import List, Dict, Any
from pathlib import Path

class DatabaseOperations:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)

    def batch_insert_customers(self, customers: List[Dict[str, Any]]):
        """Batch insert customers into database"""
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
            self.logger.info(f"Inserted {len(customers)} customers")

    def batch_insert_accounts(self, accounts: List[Dict[str, Any]]):
        """Batch insert accounts into database"""
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
            self.logger.info(f"Inserted {len(accounts)} accounts")

    def batch_insert_transactions(self, transactions: List[Dict[str, Any]]):
        """Batch insert transactions into database"""
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
            self.logger.info(f"Inserted {len(transactions)} transactions")

    def get_stats(self):
        """Get database statistics"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            stats = {}
            for table in ['customers', 'accounts', 'transactions']:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                stats[table] = cursor.fetchone()[0]

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