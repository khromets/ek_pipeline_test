#!/usr/bin/env python3
"""
Test script to demonstrate the different loading modes
Run: python test_loading_modes.py
"""

import sqlite3
from pathlib import Path

def show_current_stats():
    """Show current database statistics"""
    db_path = "data/raw/finance_data.db"

    if not Path(db_path).exists():
        print("Database doesn't exist yet")
        return

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        print("\nCURRENT DATABASE STATS:")
        print("-" * 30)

        for table in ['customers', 'accounts', 'transactions']:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{table}: {count:,} records")

        # Show timestamp distribution
        print("\nTIMESTAMP DISTRIBUTION:")
        print("-" * 30)

        for table in ['customers', 'accounts', 'transactions']:
            cursor.execute(f"""
                SELECT DATETIME(timestamp_insert) as ts, COUNT(*) as count
                FROM {table}
                GROUP BY DATETIME(timestamp_insert)
                ORDER BY ts DESC
                LIMIT 3
            """)

            results = cursor.fetchall()
            print(f"\n{table}:")
            for ts, count in results:
                print(f"  {ts}: {count:,} records")

if __name__ == "__main__":
    show_current_stats()