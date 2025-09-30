-- Finance Database Exploration Queries
-- Save this file as explore_database.sql in VS Code
-- To run: sqlite3 data/raw/finance_data.db < data/explore_database.sql

-- Show database info
.mode column
.headers on

-- 1. TABLE SCHEMAS
.echo on
SELECT 'TABLE SCHEMAS:' as info;
.schema

-- 2. RECORD COUNTS
SELECT 'RECORD COUNTS:' as info;
SELECT 'Customers:' as table_name, COUNT(*) as count FROM customers
UNION ALL
SELECT 'Accounts:', COUNT(*) FROM accounts
UNION ALL
SELECT 'Transactions:', COUNT(*) FROM transactions;

-- 3. SAMPLE DATA FROM EACH TABLE
SELECT 'SAMPLE CUSTOMERS:' as info;
SELECT customer_id, name, email, customer_type, date_joined
FROM customers
LIMIT 3;

SELECT 'SAMPLE ACCOUNTS:' as info;
SELECT account_id, customer_id, account_type, currency, balance, created_date
FROM accounts
LIMIT 3;

SELECT 'SAMPLE TRANSACTIONS:' as info;
SELECT transaction_id, account_id, transaction_type, amount, currency, category
FROM transactions
LIMIT 3;

-- 4. DATA ANALYSIS QUERIES
SELECT 'CUSTOMER TYPES DISTRIBUTION:' as info;
SELECT customer_type, COUNT(*) as count
FROM customers
GROUP BY customer_type;

SELECT 'ACCOUNT TYPES DISTRIBUTION:' as info;
SELECT account_type, COUNT(*) as count
FROM accounts
GROUP BY account_type;

SELECT 'CURRENCY DISTRIBUTION:' as info;
SELECT currency, COUNT(*) as account_count
FROM accounts
GROUP BY currency;

SELECT 'TRANSACTION TYPES:' as info;
SELECT transaction_type, COUNT(*) as count, AVG(amount) as avg_amount
FROM transactions
GROUP BY transaction_type;

-- 5. BUSINESS INTELLIGENCE QUERIES
SELECT 'TOP 5 CUSTOMERS BY TOTAL BALANCE:' as info;
SELECT c.name, c.email, SUM(a.balance) as total_balance
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
GROUP BY c.customer_id, c.name, c.email
ORDER BY total_balance DESC
LIMIT 5;

SELECT 'ACCOUNT BALANCE RANGES:' as info;
SELECT
    CASE
        WHEN balance < 1000 THEN 'Low (<$1K)'
        WHEN balance < 10000 THEN 'Medium ($1K-$10K)'
        WHEN balance < 50000 THEN 'High ($10K-$50K)'
        ELSE 'Very High (>$50K)'
    END as balance_range,
    COUNT(*) as count
FROM accounts
GROUP BY
    CASE
        WHEN balance < 1000 THEN 'Low (<$1K)'
        WHEN balance < 10000 THEN 'Medium ($1K-$10K)'
        WHEN balance < 50000 THEN 'High ($10K-$50K)'
        ELSE 'Very High (>$50K)'
    END;

-- 6. TRANSACTION ANALYSIS
SELECT 'MONTHLY TRANSACTION VOLUME:' as info;
SELECT
    strftime('%Y-%m', transaction_date) as month,
    COUNT(*) as transaction_count,
    SUM(amount) as total_volume
FROM transactions
GROUP BY strftime('%Y-%m', transaction_date)
ORDER BY month;