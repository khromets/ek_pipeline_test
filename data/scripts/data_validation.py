import sqlite3
import logging
from typing import Dict, List, Tuple

class DataValidator:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)

    def validate_referential_integrity(self) -> Dict[str, bool]:
        """Validate referential integrity between tables"""
        results = {}

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Check accounts have valid customer_id
            cursor.execute("""
                SELECT COUNT(*) FROM accounts a
                LEFT JOIN customers c ON a.customer_id = c.customer_id
                WHERE c.customer_id IS NULL
            """)
            orphaned_accounts = cursor.fetchone()[0]
            results['accounts_have_valid_customers'] = orphaned_accounts == 0

            # Check transactions have valid account_id
            cursor.execute("""
                SELECT COUNT(*) FROM transactions t
                LEFT JOIN accounts a ON t.account_id = a.account_id
                WHERE a.account_id IS NULL
            """)
            orphaned_transactions = cursor.fetchone()[0]
            results['transactions_have_valid_accounts'] = orphaned_transactions == 0

        return results

    def validate_data_ranges(self) -> Dict[str, bool]:
        """Validate data is within expected ranges"""
        results = {}

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Check for reasonable balance ranges
            cursor.execute("SELECT MIN(balance), MAX(balance) FROM accounts")
            min_balance, max_balance = cursor.fetchone()
            results['balance_ranges_reasonable'] = min_balance >= -10000 and max_balance <= 100000

            # Check for reasonable transaction amounts
            cursor.execute("SELECT MIN(amount), MAX(amount) FROM transactions")
            min_amount, max_amount = cursor.fetchone()
            results['transaction_amounts_reasonable'] = min_amount >= 0 and max_amount <= 10000

            # Check dates are reasonable
            cursor.execute("SELECT MIN(date_joined), MAX(date_joined) FROM customers")
            min_date, max_date = cursor.fetchone()
            results['customer_dates_reasonable'] = min_date and max_date

        return results

    def get_data_quality_report(self) -> Dict[str, any]:
        """Generate comprehensive data quality report"""
        report = {}

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Record counts
            for table in ['customers', 'accounts', 'transactions']:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                report[f"{table}_count"] = cursor.fetchone()[0]

            # Null checks
            cursor.execute("SELECT COUNT(*) FROM customers WHERE email IS NULL")
            report['customers_without_email'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM accounts WHERE balance IS NULL")
            report['accounts_without_balance'] = cursor.fetchone()[0]

            # Duplicate checks
            cursor.execute("SELECT COUNT(*) - COUNT(DISTINCT email) FROM customers")
            report['duplicate_customer_emails'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) - COUNT(DISTINCT account_number) FROM accounts")
            report['duplicate_account_numbers'] = cursor.fetchone()[0]

        # Referential integrity
        report['referential_integrity'] = self.validate_referential_integrity()

        # Data ranges
        report['data_ranges'] = self.validate_data_ranges()

        return report

    def print_validation_report(self):
        """Print a formatted validation report"""
        report = self.get_data_quality_report()

        print("\n" + "="*60)
        print("DATA QUALITY VALIDATION REPORT")
        print("="*60)

        print("\nRECORD COUNTS:")
        for key, value in report.items():
            if key.endswith('_count'):
                print(f"  {key.replace('_', ' ').title()}: {value:,}")

        print("\nDATA QUALITY CHECKS:")
        if report['customers_without_email'] == 0:
            print("  ✓ All customers have email addresses")
        else:
            print(f"  ✗ {report['customers_without_email']} customers missing email")

        if report['duplicate_customer_emails'] == 0:
            print("  ✓ No duplicate customer emails")
        else:
            print(f"  ✗ {report['duplicate_customer_emails']} duplicate customer emails")

        if report['duplicate_account_numbers'] == 0:
            print("  ✓ No duplicate account numbers")
        else:
            print(f"  ✗ {report['duplicate_account_numbers']} duplicate account numbers")

        print("\nREFERENTIAL INTEGRITY:")
        for check, passed in report['referential_integrity'].items():
            status = "✓" if passed else "✗"
            print(f"  {status} {check.replace('_', ' ').title()}")

        print("\nDATA RANGES:")
        for check, passed in report['data_ranges'].items():
            status = "✓" if passed else "✗"
            print(f"  {status} {check.replace('_', ' ').title()}")

        print("="*60)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    db_path = "data/raw/finance_data.db"
    validator = DataValidator(db_path)
    validator.print_validation_report()