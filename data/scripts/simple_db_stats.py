#!/usr/bin/env python3
"""
Simple database statistics script (no external dependencies)
Shows record counts and timestamp distribution for each table
Run: python data/scripts/simple_db_stats.py
"""

import sqlite3
from pathlib import Path
from datetime import datetime

def analyze_database():
    """Simple database analysis"""
    # Database path - works from any directory
    script_dir = Path(__file__).parent
    db_path = script_dir.parent / "raw" / "finance_data.db"

    if not Path(db_path).exists():
        print(f"❌ Database not found at {db_path}")
        return

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        print("🔍 DATABASE STATISTICS")
        print("=" * 50)
        print(f"Database: {db_path}")
        print(f"Analysis Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]

        print(f"\nTables: {', '.join(tables)}")

        for table in tables:
            print(f"\n{'='*20} {table.upper()} {'='*20}")

            # Total records
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total = cursor.fetchone()[0]
            print(f"📊 Total Records: {total:,}")

            # Records by timestamp_insert
            cursor.execute(f"""
                SELECT
                    DATETIME(timestamp_insert) as load_time,
                    COUNT(*) as count
                FROM {table}
                GROUP BY DATETIME(timestamp_insert)
                ORDER BY load_time DESC
            """)

            timestamp_data = cursor.fetchall()
            print(f"\n⏰ Records by Load Time:")
            for load_time, count in timestamp_data:
                percentage = (count / total * 100) if total > 0 else 0
                print(f"   {load_time}: {count:,} records ({percentage:.1f}%)")

        print("\n" + "=" * 50)

if __name__ == "__main__":
    analyze_database()