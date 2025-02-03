# Soccer Predictor DAG

## Overview
This repository contains an Apache Airflow DAG for extracting, transforming, and loading (ETL) soccer-related data from multiple sources into Snowflake for analytical purposes. The data sources include Kaggle datasets, GitHub repositories, and an SQLite database.

## Data Sources
The DAG fetches data from:
- **Kaggle**
  - European Soccer Database
  - FIFA 22 Players Dataset
- **GitHub**
  - Football data including match results, goals, and formations
- **SQLite**
  - European Soccer Database conversion to CSV
- **AWS S3**
  - Data storage and transfer

## Dependencies
### Python Packages
Ensure the following packages are installed:
- `apache-airflow`
- `apache-airflow-providers-snowflake`
- `boto3`
- `requests`
- `sqlite3`
- `kagglehub`
- `shutil`
- `csv`
- `pendulum`

### Airflow Variables
Set the following Airflow variables:
- `KAGGLE_DATASET_EUROPEAN_SOCCER_DATABASE`
- `KAGGLE_EUROPEAN_SOCCER_DATABASE_NAME`
- `KAGGLE_DATASET_FIFA_22_PLAYERS`
- `GITHUB_REPO_URL_FOOTBALL_DATA`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

### AWS S3 Configuration
- S3 Bucket: `data-lake-bdb`
- Data Path: `A53-collecte-et-stockage-des-donees/soccer-predictor/sources/`

## DAG Components
### Task Groups
1. **Download Sources**
   - Download datasets from Kaggle
   - Download football data from GitHub
   - Convert SQLite data to CSV
2. **Upload to S3**
   - Upload datasets to AWS S3
3. **Preparation for Snowflake**
   - Generate SQL queries for Snowflake table creation
4. **Snowflake Setup**
   - Configure Snowflake with required stages and file formats
5. **Load Data into Snowflake**
   - Load datasets from S3 into Snowflake tables
6. **Consolidation**
   - Process and consolidate data in Snowflake

## DAG Execution Flow
1. Download data sources
2. Upload datasets to S3
3. Generate required SQL queries
4. Configure Snowflake
5. Load data into Snowflake
6. Perform data consolidation

## How to Use
1. Ensure Airflow is running and properly configured.
2. Set the required Airflow variables.
3. Trigger the DAG manually or set a schedule.
4. Monitor the execution via Airflow UI.

