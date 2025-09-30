#!/usr/bin/env python3
"""
Comprehensive database analysis script
Shows detailed statistics for each table including timestamp analysis
Run: python data/scripts/analyze_database.py
"""

import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

class DatabaseAnalyzer:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def connect(self):
        """Create database connection"""
        return sqlite3.connect(self.db_path)

    def get_all_tables(self):
        """Get list of all tables in the database"""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
        return tables

    def get_table_schema(self, table_name: str):
        """Get schema information for a table"""
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = cursor.fetchall()
        return columns

    def analyze_table(self, table_name: str):
        """Comprehensive analysis of a single table"""
        with self.connect() as conn:
            cursor = conn.cursor()

            print(f"\n{'='*80}")
            print(f"TABLE: {table_name.upper()}")
            print(f"{'='*80}")

            # 1. Total record count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            total_records = cursor.fetchone()[0]
            print(f"📊 TOTAL RECORDS: {total_records:,}")

            if total_records == 0:
                print("   (Table is empty)")
                return

            # 2. Schema information
            schema = self.get_table_schema(table_name)
            print(f"\n📋 SCHEMA:")
            print("   Column Name          | Type      | Not Null | Default   | Primary Key")
            print("   " + "-" * 70)
            for col in schema:
                cid, name, type_, notnull, default, pk = col
                print(f"   {name:<20} | {type_:<9} | {bool(notnull):<8} | {str(default or 'None'):<9} | {bool(pk)}")

            # 3. Check if timestamp_insert column exists
            timestamp_columns = [col[1] for col in schema if 'timestamp' in col[1].lower()]

            if timestamp_columns:
                # 4. Records by timestamp_insert
                print(f"\n⏰ RECORDS BY TIMESTAMP_INSERT:")
                cursor.execute(f"""
                    SELECT
                        DATETIME(timestamp_insert) as load_timestamp,
                        COUNT(*) as record_count,
                        ROUND(COUNT(*) * 100.0 / {total_records}, 2) as percentage
                    FROM {table_name}
                    GROUP BY DATETIME(timestamp_insert)
                    ORDER BY load_timestamp DESC
                """)

                timestamp_data = cursor.fetchall()
                print("   Load Timestamp       | Records   | Percentage")
                print("   " + "-" * 45)
                for ts, count, pct in timestamp_data:
                    print(f"   {ts} | {count:8,} | {pct:7.2f}%")

                # 5. Date range of timestamp_insert
                cursor.execute(f"""
                    SELECT
                        MIN(timestamp_insert) as earliest,
                        MAX(timestamp_insert) as latest
                    FROM {table_name}
                """)
                earliest, latest = cursor.fetchone()
                print(f"\n📅 TIMESTAMP RANGE:")
                print(f"   Earliest: {earliest}")
                print(f"   Latest:   {latest}")

            else:
                print(f"\n⚠️  No timestamp_insert column found")

            # 6. Sample data (first 3 records)
            print(f"\n🔍 SAMPLE DATA (First 3 records):")
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")

            # Get column names
            column_names = [description[0] for description in cursor.description]
            rows = cursor.fetchall()

            if rows:
                # Create DataFrame for better formatting
                df = pd.DataFrame(rows, columns=column_names)
                print(df.to_string(index=False, max_cols=8, max_colwidth=20))
            else:
                print("   No data to display")

            # 7. Table-specific analysis
            self.table_specific_analysis(table_name, cursor)

    def table_specific_analysis(self, table_name: str, cursor):
        """Table-specific analysis based on table name"""

        if table_name == "customers":
            print(f"\n👥 CUSTOMER-SPECIFIC ANALYSIS:")

            # Customer types distribution
            cursor.execute("""
                SELECT customer_type, COUNT(*) as count,
                       ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customers), 2) as percentage
                FROM customers
                GROUP BY customer_type
                ORDER BY count DESC
            """)
            customer_types = cursor.fetchall()
            print("   Customer Type  | Count     | Percentage")
            print("   " + "-" * 35)
            for ctype, count, pct in customer_types:
                print(f"   {ctype:<14} | {count:8,} | {pct:7.2f}%")

        elif table_name == "accounts":
            print(f"\n🏦 ACCOUNT-SPECIFIC ANALYSIS:")

            # Account types distribution
            cursor.execute("""
                SELECT account_type, COUNT(*) as count,
                       ROUND(AVG(balance), 2) as avg_balance,
                       ROUND(SUM(balance), 2) as total_balance
                FROM accounts
                GROUP BY account_type
                ORDER BY count DESC
            """)
            account_types = cursor.fetchall()
            print("   Account Type | Count     | Avg Balance    | Total Balance")
            print("   " + "-" * 55)
            for atype, count, avg_bal, total_bal in account_types:
                print(f"   {atype:<12} | {count:8,} | ${avg_bal:11,.2f} | ${total_bal:12,.2f}")

            # Currency distribution
            cursor.execute("""
                SELECT currency, COUNT(*) as count,
                       ROUND(SUM(balance), 2) as total_balance
                FROM accounts
                GROUP BY currency
                ORDER BY count DESC
            """)
            currencies = cursor.fetchall()
            print("\n   Currency | Count     | Total Balance")
            print("   " + "-" * 35)
            for curr, count, total_bal in currencies:
                print(f"   {curr:<8} | {count:8,} | ${total_bal:12,.2f}")

        elif table_name == "transactions":
            print(f"\n💰 TRANSACTION-SPECIFIC ANALYSIS:")

            # Transaction types distribution
            cursor.execute("""
                SELECT transaction_type, COUNT(*) as count,
                       ROUND(AVG(amount), 2) as avg_amount,
                       ROUND(SUM(amount), 2) as total_amount
                FROM transactions
                GROUP BY transaction_type
                ORDER BY total_amount DESC
            """)
            transaction_types = cursor.fetchall()
            print("   Transaction Type | Count      | Avg Amount     | Total Amount")
            print("   " + "-" * 60)
            for ttype, count, avg_amt, total_amt in transaction_types:
                print(f"   {ttype:<16} | {count:9,} | ${avg_amt:11,.2f} | ${total_amt:12,.2f}")

            # Monthly transaction volume
            cursor.execute("""
                SELECT
                    strftime('%Y-%m', transaction_date) as month,
                    COUNT(*) as transaction_count,
                    ROUND(SUM(amount), 2) as total_volume
                FROM transactions
                GROUP BY strftime('%Y-%m', transaction_date)
                ORDER BY month DESC
                LIMIT 6
            """)
            monthly_data = cursor.fetchall()
            print("\n   Month    | Transactions | Total Volume")
            print("   " + "-" * 40)
            for month, count, volume in monthly_data:
                print(f"   {month}     | {count:11,} | ${volume:12,.2f}")

    def analyze_all_tables(self):
        """Analyze all tables in the database"""
        print("🔍 DATABASE ANALYSIS REPORT")
        print("=" * 80)
        print(f"Database: {self.db_path}")
        print(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        tables = self.get_all_tables()

        if not tables:
            print("No tables found in database")
            return

        print(f"\nTables found: {', '.join(tables)}")

        # Overall database statistics
        print(f"\n📈 OVERALL DATABASE STATISTICS:")
        total_records = 0

        with self.connect() as conn:
            cursor = conn.cursor()

            print("   Table         | Records")
            print("   " + "-" * 25)

            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                total_records += count
                print(f"   {table:<13} | {count:8,}")

            print("   " + "-" * 25)
            print(f"   TOTAL         | {total_records:8,}")

        # Analyze each table
        for table in tables:
            self.analyze_table(table)

        print(f"\n{'='*80}")
        print("✅ ANALYSIS COMPLETE")
        print(f"{'='*80}")

    def generate_summary_report(self):
        """Generate a concise summary report"""
        tables = self.get_all_tables()

        print(f"\n📋 QUICK SUMMARY:")
        print("-" * 40)

        with self.connect() as conn:
            for table in tables:
                cursor = conn.cursor()

                # Basic count
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                total = cursor.fetchone()[0]

                # Timestamp distribution count
                cursor.execute(f"""
                    SELECT COUNT(DISTINCT DATETIME(timestamp_insert))
                    FROM {table}
                    WHERE timestamp_insert IS NOT NULL
                """)
                unique_timestamps = cursor.fetchone()[0]

                print(f"{table}: {total:,} records across {unique_timestamps} load(s)")

def main():
    # Database path - works from any directory
    script_dir = Path(__file__).parent
    db_path = script_dir.parent / "raw" / "finance_data.db"

    # Check if database exists
    if not Path(db_path).exists():
        print(f"❌ Database not found at {db_path}")
        print("Make sure you've run the data generation script first!")
        return

    # Create analyzer
    analyzer = DatabaseAnalyzer(db_path)

    # Run analysis
    analyzer.analyze_all_tables()

    # Generate summary
    analyzer.generate_summary_report()

if __name__ == "__main__":
    main()