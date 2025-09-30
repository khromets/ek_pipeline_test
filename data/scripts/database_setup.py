import sqlite3
import logging
from pathlib import Path

class DatabaseSetup:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)

    def create_database(self):
        """Create SQLite database and tables with proper schema"""
        try:
            # Ensure the directory exists
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Create customers table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS customers (
                        customer_id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        email TEXT UNIQUE,
                        phone TEXT,
                        address TEXT,
                        date_joined DATE,
                        customer_type TEXT,
                        timestamp_insert DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Create accounts table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS accounts (
                        account_id TEXT PRIMARY KEY,
                        customer_id TEXT,
                        account_number TEXT UNIQUE,
                        account_type TEXT,
                        iban TEXT,
                        balance DECIMAL(15,2),
                        currency TEXT,
                        created_date DATE,
                        timestamp_insert DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
                    )
                """)

                # Create transactions table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS transactions (
                        transaction_id TEXT PRIMARY KEY,
                        account_id TEXT,
                        transaction_date DATETIME,
                        transaction_type TEXT,
                        amount DECIMAL(15,2),
                        currency TEXT,
                        description TEXT,
                        merchant TEXT,
                        category TEXT,
                        balance_after DECIMAL(15,2),
                        timestamp_insert DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (account_id) REFERENCES accounts(account_id)
                    )
                """)

                # Create indexes for better performance
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_accounts_customer_id ON accounts(customer_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions(account_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)")

                conn.commit()
                self.logger.info(f"Database created successfully at {self.db_path}")

        except Exception as e:
            self.logger.error(f"Error creating database: {e}")
            raise

    def get_table_info(self):
        """Get information about existing tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            tables = ['customers', 'accounts', 'transactions']
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table}: {count} records")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    db_path = "../data/raw/finance_data.db"
    setup = DatabaseSetup(db_path)
    setup.create_database()
    setup.get_table_info()