#!/usr/bin/env python3
"""
Interactive database exploration script
Run with: python data/scripts/query_database.py
"""

import sqlite3
import pandas as pd
from pathlib import Path

class DatabaseExplorer:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def connect(self):
        """Create database connection"""
        return sqlite3.connect(self.db_path)

    def show_tables(self):
        """List all tables in the database"""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            print("Tables in database:")
            for table in tables:
                print(f"  - {table[0]}")
            return [table[0] for table in tables]

    def show_schema(self, table_name: str):
        """Show schema for a specific table"""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()

            print(f"\nSchema for '{table_name}':")
            print("Column Name      | Type    | Not Null | Default | Primary Key")
            print("-" * 65)
            for col in columns:
                cid, name, type_, notnull, default, pk = col
                print(f"{name:<16} | {type_:<7} | {bool(notnull):<8} | {default or 'None':<7} | {bool(pk)}")

    def count_records(self):
        """Count records in all tables"""
        tables = self.show_tables()
        print("\nRecord counts:")

        with self.connect() as conn:
            for table in tables:
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table}: {count:,} records")

    def sample_data(self, table_name: str, limit: int = 5):
        """Show sample data from a table"""
        with self.connect() as conn:
            df = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT {limit}", conn)
            print(f"\nSample data from '{table_name}':")
            print(df.to_string(index=False))
            return df

    def custom_query(self, query: str):
        """Execute a custom SQL query"""
        with self.connect() as conn:
            try:
                df = pd.read_sql_query(query, conn)
                print(f"\nQuery results:")
                print(df.to_string(index=False))
                return df
            except Exception as e:
                print(f"Error executing query: {e}")
                return None

    def business_insights(self):
        """Run some business intelligence queries"""
        print("\n" + "="*60)
        print("BUSINESS INSIGHTS")
        print("="*60)

        queries = {
            "Customer Distribution by Type": """
                SELECT customer_type, COUNT(*) as count,
                       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customers), 2) as percentage
                FROM customers
                GROUP BY customer_type
            """,

            "Account Balance Summary": """
                SELECT currency, COUNT(*) as accounts,
                       MIN(balance) as min_balance,
                       AVG(balance) as avg_balance,
                       MAX(balance) as max_balance
                FROM accounts
                GROUP BY currency
            """,

            "Top 5 Customers by Total Balance": """
                SELECT c.name, c.email, SUM(a.balance) as total_balance
                FROM customers c
                JOIN accounts a ON c.customer_id = a.customer_id
                GROUP BY c.customer_id, c.name, c.email
                ORDER BY total_balance DESC
                LIMIT 5
            """,

            "Transaction Summary by Type": """
                SELECT transaction_type,
                       COUNT(*) as count,
                       AVG(amount) as avg_amount,
                       SUM(amount) as total_amount
                FROM transactions
                GROUP BY transaction_type
                ORDER BY total_amount DESC
            """
        }

        for title, query in queries.items():
            print(f"\n{title}:")
            print("-" * len(title))
            self.custom_query(query)

def main():
    # Database path
    db_path = "data/raw/finance_data.db"

    # Check if database exists
    if not Path(db_path).exists():
        print(f"Database not found at {db_path}")
        print("Make sure you've run the data generation script first!")
        return

    # Create explorer
    explorer = DatabaseExplorer(db_path)

    print("Finance Database Explorer")
    print("=" * 40)

    # Show basic info
    explorer.show_tables()
    explorer.count_records()

    # Show schemas
    for table in ['customers', 'accounts', 'transactions']:
        explorer.show_schema(table)

    # Show sample data
    for table in ['customers', 'accounts', 'transactions']:
        explorer.sample_data(table, 3)

    # Business insights
    explorer.business_insights()

    print("\n" + "="*60)
    print("To run custom queries, use:")
    print("explorer.custom_query('YOUR SQL QUERY HERE')")
    print("="*60)

if __name__ == "__main__":
    main()