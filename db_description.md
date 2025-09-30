1. generate_finance_data_enhanced.py - Main enhanced
  script
  2. db_operations_enhanced.py - Enhanced database
  operations
  3. test_loading_modes.py - Test current database state

  ---
  🔧 Three Loading Modes:

  1. REPLACE Mode (Default)

  python generate_finance_data_enhanced.py --mode replace
  --customers 500 --accounts-per-customer 3
  --transactions-per-account 25
  - Behavior: Deletes ALL existing data, replaces with new
  data
  - Result: 500 customers, 1,500 accounts, 37,500
  transactions (total)

  2. INSERT Mode (Add New Data)

  python generate_finance_data_enhanced.py --mode insert
  --customers 100 --accounts-per-customer 2
  --transactions-per-account 15
  - Behavior: Adds NEW data to existing data (no duplicates)
  - Current: 100 customers, 200 accounts, 2,000 transactions
  - After: 200 customers, 400 accounts, 5,000 transactions
  (+100 customers, +200 accounts, +3,000 transactions)

  3. MERGE Mode (Smart Upsert)

  python generate_finance_data_enhanced.py --mode merge
  --customers 150 --accounts-per-customer 3
  --transactions-per-account 20
  - Behavior: Ensures EXACTLY the specified counts
  - Current: 100 customers, 200 accounts, 2,000 transactions

  - After: 150 customers, 450 accounts, 9,000 transactions
  (adds 50 customers, adjusts accounts/transactions per
  customer)

  ---
  📊 Usage Examples:

  Check current state:
  source py_env/bin/activate
  python test_loading_modes.py

  Add 50 more customers with 2 accounts each:
  source py_env/bin/activate
  cd pulling_raw_data/scripts
  python generate_finance_data_enhanced.py --mode insert
  --customers 50 --accounts-per-customer 2
  --transactions-per-account 20

  ----

   ✅ Database Analysis Scripts Created!

  I've created two analysis scripts for you:

  📊 Comprehensive Analysis Script:

  File: data/scripts/analyze_database.py

  Features:
  - ✅ Complete table analysis with schema, record counts,
  timestamp distribution
  - ✅ Business intelligence insights (customer types, account
  balances, transaction patterns)
  - ✅ Monthly transaction volume analysis
  - ✅ Sample data display for each table
  - ✅ Beautiful formatting with pandas

  Usage:
  source py_env/bin/activate
  python data/scripts/analyze_database.py

  📈 Simple Stats Script:

  File: data/scripts/simple_db_stats.py

  Features:
  - ✅ Quick record counts by table
  - ✅ Timestamp distribution analysis
  - ✅ No external dependencies (just SQLite)
  - ✅ Fast execution

  Usage:
  source py_env/bin/activate
  python data/scripts/simple_db_stats.py