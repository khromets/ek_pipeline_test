# Project 

Pipeline for learning

# Project Plan

## Step 1. Pull public data from API (done)

* let's use Fake data from https://fakerjs.dev/ especially I'm interested in Finance data
* put scrips/files related to step 1 under folder /pulling_raw_data/


1. I would like to use a public source of data
2. I would like to pull the data by using API
3. I want to have a set of scripts that pull raw data

## Step 2. Storing raw data (done)

store all elements (files, scripts, dbs) related to storing data from API in the folder data/

1. I would like to store that raw data from Step 1 in a simple database probably using SQLight

## Step 3. Scheduling data (done)

I want to schedule scripts from step 1 and 2 to populate data on a daily or weekly basis by using Airflow.

* main script to execute that loads data:
python pulling_raw_data/scripts/generate_finance_data_enhanced.py --mode insert --customers 10 --accounts-per-customer 2 --transactions-per-account 10

* I want to load data daily at 11am eastern time (Toronto time)

## Step 4. Process data

1. I want to build an ELT pipeline by using dbt core
2. I want to schedule it by using Airflow again

## Step 5. Presenting data

1. I want to build a few visualisation based on the processed data

# Tools

List of tools that I want to use:
1. Airflow
2. dbt
3. Python for scripting
4. SQLite

run python in py_env/ environment. you'll need to activate it first.

# APIs

Here are a few suggestion for public APIs that can be used:
1. PandaScore APIs for real-time esports statistics - https://developers.pandascore.co/docs/introduction\
2. Fake data from https://fakerjs.dev/ especially I'm interested in Finance data
2. Bitwarden Public API https://bitwarden.com/help/api/


--------

⏺ Step 1 & 2 Implementation Complete!

  ✅ Successfully implemented the complete data pipeline:

  What was created:

  Structure:
  /pulling_raw_data/          # Step 1: Data generation
  ├── scripts/
  │   ├── generate_finance_data.py    # Main orchestrator
  │   ├── data_models.py              # Pydantic data models
  │   └── config.py                   # Configuration
  ├── backup/                         # JSON backups
  │   ├── customers.json
  │   ├── accounts.json  
  │   └── transactions.json
  └── requirements.txt

  /data/                      # Step 2: Storage
  ├── raw/
  │   └── finance_data.db            # SQLite database
  ├── logs/
  │   └── generation_logs.txt        # Process logs
  └── scripts/
      ├── database_setup.py          # Schema creation
      ├── db_operations.py           # Database utilities
      └── data_validation.py         # Quality checks

  Test Results:
  - ✅ Generated 100 customers, 200 accounts, 2,000
  transactions
  - ✅ All referential integrity checks passed
  - ✅ Data quality validation successful
  - ✅ JSON backups created automatically
  - ✅ SQLite database ready for dbt (Step 4)

  Usage:
  source py_env/bin/activate
  cd pulling_raw_data/scripts
  python generate_finance_data.py --customers 1000
  --accounts-per-customer 2 --transactions-per-account 50

  The pipeline is now ready for Step 3 (Airflow scheduling)
  and Step 4 (dbt processing)!