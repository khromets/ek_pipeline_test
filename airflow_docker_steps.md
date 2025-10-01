# Option 1: Docker Compose - Major Implementation Steps
Prerequisites (One-time setup) (done)
Install Docker Desktop (if not already installed)
Download from docker.com
Verify: docker --version and docker compose version

# Implementation Steps
## Step 1: Create Airflow Project Structure (5 min) (done)
* Create /airflow/ directory
* Create subdirectories: dags/, logs/, plugins/, config/
* Add .gitignore for Airflow-specific files

## Step 2: Download & Configure Docker Compose (10 min) (done)
* Download official Airflow docker-compose.yaml from Apache Airflow (done)
* Create .env file with: (done)
    * Airflow UID/GID settings
    * Executor type (LocalExecutor for simplicity)
    * Connections/secrets if needed
* Configure volume mounts to access your project files: (done)
    * Mount /pulling_raw_data/ (read-only)
    * Mount /data/ (read-write for DB)
    * Mount py_env/ dependencies

## Step 3: Create Custom Dockerfile (10 min) (done)
Extend official Airflow image
Install your project dependencies (faker, pydantic)
Set up timezone (America/Toronto)
Optional: Copy project files into image

## Step 4: Create the DAG File (15 min) (done)
Create airflow/dags/finance_data_daily.py
Define DAG with:
Schedule: 0 11 * * * with Toronto timezone
Tasks using BashOperator to:
Activate py_env
Run generate_finance_data_enhanced.py
Validate data (optional)
Set task dependencies

## Step 5: Initialize Airflow (5 min)
Run docker compose up airflow-init (one-time DB setup)
Wait for initialization to complete

## Step 6: Start Airflow Services (2 min)
Run docker compose up -d
Services start: webserver, scheduler, postgres
Access web UI at http://localhost:8080
Default login: airflow / airflow

## Step 7: Test & Validate (10 min)
Enable your DAG in web UI
Manually trigger first run
Monitor task logs
Verify data appears in SQLite DB
Check row counts match expectations

## Step 8: Enable Scheduled Runs (2 min) (done)
Confirm DAG is unpaused
Wait for 9am next day, or adjust schedule for testing
Monitor automated runs
Total Time Estimate: ~1 hour
Key Files You'll Create:
/airflow/
├── docker-compose.yaml       # Airflow services definition
├── Dockerfile               # Custom image with your deps
├── .env                     # Configuration variables
├── requirements.txt         # Python dependencies
└── dags/
    └── finance_data_daily.py  # Your DAG definition
Day-to-Day Usage After Setup:
Start: docker compose up -d (in /airflow/ folder)
Stop: docker compose down
View logs: Web UI or docker compose logs -f
Restart: docker compose restart