from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

import sqlite3
import os
import kagglehub
import shutil
import pendulum
import csv
import boto3
import requests

KAGGLE_DATASET_EUROPEAN_SOCCER_DATABASE = Variable.get('KAGGLE_DATASET_EUROPEAN_SOCCER_DATABASE')
KAGGLE_EUROPEAN_SOCCER_DATABASE_NAME = Variable.get('KAGGLE_EUROPEAN_SOCCER_DATABASE_NAME')
KAGGLE_DATASET_FIFA_22_PLAYERS = Variable.get('KAGGLE_DATASET_FIFA_22_PLAYERS')
GITHUB_REPO_URL_FOOTBALL_DATA = Variable.get('GITHUB_REPO_URL_FOOTBALL_DATA')

AWS_ACCESS_KEY_ID = Variable.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')

EUROPEAN_SOCCER_DB_FOLDER_NAME = 'european_soccer_db'
FIFA_22_PLAYERS_DATASET_FOLDER_NAME = 'fifa_22_players_dataset'
FOOTBALL_DATA_FOLDER_NAME = 'football_data'

S3_BUCKET_NAME = 'data-lake-bdb'
S3_SOURCES_PATH = 'A53-collecte-et-stockage-des-donees/soccer-predictor/sources/'

def copy_files_between_paths(source_directory, destination_directory):

    # Copy each file from source to destination
    for file_name in os.listdir(source_directory):
        full_file_path = os.path.join(source_directory, file_name)

        # Check if it's a file (not a directory)
        if os.path.isfile(full_file_path):
            shutil.copy(full_file_path, destination_directory)
            print(f"Copied {full_file_path} to {destination_directory}")

            os.remove(full_file_path)
            print(f"File {full_file_path} deleted")


def get_full_path_ds_location(folder_name):
    # Get the current directory
    current_directory = os.getcwd()

    # Construct the full path for the new folder
    folder_path = os.path.join(current_directory, folder_name)

    # Create the folder
    os.makedirs(folder_path, exist_ok=True)  # 'exist_ok=True' prevents error if the folder already exists

    return folder_path

def get_data_sources(kaggle_data_source, folder_location, **kwargs):
    folder_path = get_full_path_ds_location(folder_location)

    # Download latest version
    path = kagglehub.dataset_download(kaggle_data_source, force_download=True)

    print("Path to dataset files:", path)

    # Copy the file(s)
    copy_files_between_paths(path, folder_path)

def download_files(folder, files_report):
    repo_url = f"{GITHUB_REPO_URL_FOOTBALL_DATA}/{folder}"

    # Create a directory to store the downloaded files
    os.makedirs(FOOTBALL_DATA_FOLDER_NAME, exist_ok=True)

    # Fetch the contents of the repository
    response = requests.get(repo_url)
    files = response.json()

    # Download each file
    for file in files:
        if file['type'] == 'file':
            file_url = file['download_url']
            file_name = file['name']

            if file_name.endswith('.csv'):
                files_report.append(file_name.replace('.csv', ''))

            file_content = requests.get(file_url).content

            # Save the file
            with open(os.path.join(FOOTBALL_DATA_FOLDER_NAME, file_name), 'wb') as f:
                f.write(file_content)
            print(f"Downloaded: {file_name}")

    print("All files have been downloaded.")
    return files_report

def get_data_source_github(**kwargs):
    folders = ['results', 'goals_time', 'formations']
    files_report = []

    for folder in folders:
        download_files(folder, files_report)

    return files_report

def export_table_to_csv(table_name, cursor, folder_path):
    table_name = f'{table_name}'
    with open(f'{folder_path}/{table_name}.csv', 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        cursor.execute(f"SELECT * FROM {table_name}")

        # Write header
        writer.writerow([desc[0] for desc in cursor.description])

        # Write data rows
        writer.writerows(cursor.fetchall())

def sqlite_to_csv(**kwargs):
    folder_path = get_full_path_ds_location(EUROPEAN_SOCCER_DB_FOLDER_NAME)

    # Connect to SQLite database
    conn = sqlite3.connect(f'{folder_path}/{KAGGLE_EUROPEAN_SOCCER_DATABASE_NAME}')
    cursor = conn.cursor()

    # Export a table to CSV
    tables = ['Country', 'League', 'Match', 'Player', 'Player_Attributes', 'Team', 'Team_Attributes']
    for table in tables:
        export_table_to_csv(table, cursor, folder_path)

    conn.close()

def delete_files(file_paths):
    for file_path in file_paths:
        # if os.path.exists(file_path):
        os.remove(file_path)
            # print(f"File deleted {file_path}")

def list_files_in_folder(folder_path):
    try:
        # List all files in the folder
        files = [file for file in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, file))]
        return files
    except FileNotFoundError:
        print(f"The folder '{folder_path}' does not exist.")
        return []
    except PermissionError:
        print(f"Permission denied for accessing the folder '{folder_path}'.")
        return []


def upload_files_to_s3(local_files_folder, s3_folder_name, **kwargs):
    # Get the current directory
    folder_path = get_full_path_ds_location (local_files_folder)

    files = list_files_in_folder(folder_path)

    file_paths = []
    for file_name in files:
        file_paths.append(os.path.join(folder_path, file_name))

    bucket_name = S3_BUCKET_NAME
    s3_folder = f'{S3_SOURCES_PATH}{s3_folder_name}'

    aws_access_key_id = AWS_ACCESS_KEY_ID
    aws_secret_access_key = AWS_SECRET_ACCESS_KEY

    # Initialize the S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name='ca-central-1',
    )

    for file_path in file_paths:
        try:
            # Get the file name
            file_name = os.path.basename(file_path)

            # Define the S3 key (folder path + file name)
            s3_key = f"{s3_folder.rstrip('/')}/{file_name}"

            # Upload the file
            s3_client.upload_file(file_path, bucket_name, s3_key)
            print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")

        except Exception as e:
            print(f"Failed to upload {file_path}: {e}")

    delete_files(file_paths)

def query_setup_snowflake():
    query = f"""
        CREATE DATABASE IF NOT EXISTS LANDING_ZONE;        

        USE SCHEMA LANDING_ZONE.PUBLIC;

        CREATE STAGE IF NOT EXISTS bdb_data_lake_s3_stage
        STORAGE_INTEGRATION = s3_integration_data_lake_bdb
        URL = 's3://data-lake-bdb/';

        USE SCHEMA LANDING_ZONE.PUBLIC;

        CREATE OR REPLACE FILE FORMAT my_csv_format
            TYPE = CSV
            PARSE_HEADER = TRUE
            FIELD_DELIMITER = ','
            TRIM_SPACE = TRUE
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('');
            
        CREATE OR REPLACE FILE FORMAT parquet_format
            TYPE = PARQUET;            
    """

    return query

def get_table_creation_queries(snow_schema_name, lst_tables, topic, **kwargs):
    tables = lst_tables

    query = f"""
        USE DATABASE LANDING_ZONE;
        
        CREATE SCHEMA IF NOT EXISTS LANDING_ZONE.{snow_schema_name};
    """

    for table in tables:
        query += f"""
            CREATE OR REPLACE TABLE LANDING_ZONE.{snow_schema_name}.{table.upper().replace('-', '_')}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION=>'@LANDING_ZONE.PUBLIC.BDB_DATA_LAKE_S3_STAGE/{S3_SOURCES_PATH}{topic}/{table}.csv',
                        FILE_FORMAT=>'my_csv_format'
                    )
                )
            );
            
            COPY INTO LANDING_ZONE.{snow_schema_name}.{table.upper().replace('-', '_')}
            FROM '@LANDING_ZONE.PUBLIC.BDB_DATA_LAKE_S3_STAGE/{S3_SOURCES_PATH}{topic}/{table}.csv'
            FILE_FORMAT = my_csv_format
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
                        
        """
    return query

def get_query_load_parquet_file(schema_name, file_name, **kwargs):
    query = f"""
        USE DATABASE LANDING_ZONE;
        
        CREATE SCHEMA IF NOT EXISTS LANDING_ZONE.{schema_name};
              
        CREATE OR REPLACE TABLE LANDING_ZONE.{schema_name}.GAMES
        USING TEMPLATE (
          SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
          FROM TABLE(
            INFER_SCHEMA(
              LOCATION=>'@LANDING_ZONE.PUBLIC.BDB_DATA_LAKE_S3_STAGE/{S3_SOURCES_PATH}{FOOTBALL_DATA_FOLDER_NAME}/{file_name}',
              FILE_FORMAT=>'parquet_format'
            )
          )
        );        
        
        COPY INTO LANDING_ZONE.{schema_name}.GAMES
        FROM '@LANDING_ZONE.PUBLIC.BDB_DATA_LAKE_S3_STAGE/{S3_SOURCES_PATH}{FOOTBALL_DATA_FOLDER_NAME}/{file_name}'
        FILE_FORMAT = parquet_format
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;        
    """
    return query

def generate_query_for_football_data(**kwargs):
    # Retrieve `files_report` from the previous task using XCom.
    ti = kwargs['ti']
    print("ti", ti)
    files_report = ti.xcom_pull(task_ids='download_sources.download_football_data_from_github')

    # Example of using `files_report`.
    print(f"Files Report: {files_report}")

    # Execute Snowflake operations (adjust SQL logic as needed).
    sql_queries = get_table_creation_queries(
        snow_schema_name='FOOTBALL_DATA',
        lst_tables=files_report,  # Use the retrieved `files_report`.
        topic=FOOTBALL_DATA_FOLDER_NAME
    )

    # Push the generated SQL queries to XCom
    ti.xcom_push(key='sql_queries', value=sql_queries)
    print("SQL queries pushed to XCom.")

def consolidation_query_european_soccer_db():
    query = f"""
        --SPAIN MATCHES
        CREATE TABLE STAGING.SPAIN.MATCH AS
            WITH SPAIN_DATA AS (
                select "id" AS id 
                from LANDING_ZONE.EUROPEAN_SOCCER_DB.COUNTRY 
                where "name" = 'Spain'
            ),
            SPAIN_MATCHES AS (
                select * 
                from LANDING_ZONE.EUROPEAN_SOCCER_DB.match 
                where "country_id" = (select id from SPAIN_DATA)
                and "league_id" = (
                    select "id" from LANDING_ZONE.EUROPEAN_SOCCER_DB.LEAGUE where "country_id" = (select id from SPAIN_DATA)
                ) 
            )
            SELECT * FROM SPAIN_MATCHES;
            
        --SPAIN PLAYERS
        CREATE TABLE STAGING.SPAIN.PLAYER AS
            WITH players_in_match AS (
                SELECT distinct player
                FROM STAGING.SPAIN.MATCH
                UNPIVOT(player FOR player_position IN (
                    "home_player_1",
                    "home_player_2",
                    "home_player_3",
                    "home_player_4",
                    "home_player_5",
                    "home_player_6",
                    "home_player_7",
                    "home_player_8",
                    "home_player_9",
                    "home_player_10",
                    "home_player_11",
                    "away_player_1",
                    "away_player_2",
                    "away_player_3",
                    "away_player_4",
                    "away_player_5",
                    "away_player_6",
                    "away_player_7",
                    "away_player_8",
                    "away_player_9",
                    "away_player_10",
                    "away_player_11"
                )) AS unpivoted_data
            )
            SELECT * FROM LANDING_ZONE.EUROPEAN_SOCCER_DB.PLAYER
            WHERE "player_api_id" IN (SELECT player FROM players_in_match); 
            
        --SPAIN PLAYERS ATTRINUTES
        CREATE TABLE STAGING.SPAIN.PLAYER_ATTRIBUTES AS(
            select * from LANDING_ZONE.EUROPEAN_SOCCER_DB.PLAYER_ATTRIBUTES
            where "player_api_id" in (select distinct "player_api_id" from STAGING.SPAIN.PLAYER)
            and "player_fifa_api_id" in (select distinct "player_fifa_api_id" from STAGING.SPAIN.PLAYER)
        );
        
        --SPAIN TEAMS
        CREATE TABLE STAGING.SPAIN.TEAM AS
            select * from LANDING_ZONE.EUROPEAN_SOCCER_DB.TEAM where "team_api_id" in (
                select distinct * from (
                    SELECT "home_team_api_id" FROM STAGING.SPAIN.MATCH 
                    UNION
                    SELECT "away_team_api_id" FROM STAGING.SPAIN.MATCH 
                )
            );          
            
        --SPAIN TEAMS ATTRINUTES
        CREATE TABLE STAGING.SPAIN.TEAM_ATTRIBUTES AS
            select * from LANDING_ZONE.EUROPEAN_SOCCER_DB.TEAM_ATTRIBUTES
            where "team_api_id" in (select distinct "team_api_id" from STAGING.SPAIN.TEAM);        
    """
    return query

def consolidation_query_fifa_22_players_dataset():
    query = f"""
        CREATE TABLE STAGING.SPAIN.PLAYER_ATTRIBUTES_EXTENDED AS
            WITH CONSOLIDATION AS (
                select 'FIFA_15' as source, * from LANDING_ZONE.FIFA_22_PLAYERS_DATASET.PLAYERS_15
                UNION
                select 'FIFA_16' as source, * from LANDING_ZONE.FIFA_22_PLAYERS_DATASET.PLAYERS_16
                UNION
                select 'FIFA_17' as source, * from LANDING_ZONE.FIFA_22_PLAYERS_DATASET.PLAYERS_17
                UNION
                select 'FIFA_18' as source, * from LANDING_ZONE.FIFA_22_PLAYERS_DATASET.PLAYERS_18
                UNION
                select 'FIFA_19' as source, * from LANDING_ZONE.FIFA_22_PLAYERS_DATASET.PLAYERS_19
                UNION
                select 'FIFA_20' as source, * from LANDING_ZONE.FIFA_22_PLAYERS_DATASET.PLAYERS_20
                UNION
                select 'FIFA_21' as source, * from LANDING_ZONE.FIFA_22_PLAYERS_DATASET.PLAYERS_21
                UNION
                select 'FIFA_22' as source, * from LANDING_ZONE.FIFA_22_PLAYERS_DATASET.PLAYERS_22
            )
            select * from CONSOLIDATION where "sofifa_id" in (select "player_fifa_api_id" from STAGING.SPAIN.PLAYER);     
    """
    return query


def consolidation_query_football_data():
    query = f"""
        CREATE TABLE STAGING.SPAIN.CHAMPIONS_LEAGUE_MATCHES AS 
            WITH normalized_spain_team_names AS (
                SELECT 
                    "team_api_id",
                    "team_fifa_api_id",
                    "team_long_name", 
                    TRANSLATE(
                        TRIM(
                            REGEXP_REPLACE(UPPER("team_long_name"), '(CF|RCD|CA |RC |CD |FC | FC|UD | UD|SD |LAS )', '')
                        ),
                        'ÁÉÍÓÚáéíóúÑñ', 
                        'AEIOUaeiouNn'        
                    )AS simplified_team_name
                FROM STAGING.SPAIN.TEAM
            ),
            normalized_c_l_team_names AS (
                SELECT *,
                    TRANSLATE(
                        TRIM(
                            REGEXP_REPLACE(UPPER("scoring_team"), '(CF|RCD|CA |RC |CD |FC | FC|UD | UD|SD |LAS )', '')
                        ),
                        'ÁÉÍÓÚáéíóúÑñ', 
                        'AEIOUaeiouNn'        
                    )AS simplified_team_name
                FROM LANDING_ZONE.FOOTBALL_DATA.CHAMPIONS_LEAGUE
            )
            select T."team_api_id", T."team_fifa_api_id" , C_L.* EXCLUDE simplified_team_name  from normalized_c_l_team_names C_L
            join normalized_spain_team_names T
            ON C_L.simplified_team_name = T.simplified_team_name; 
            
        CREATE TABLE STAGING.SPAIN.CHAMPIONS_LEAGUE_PLAYERS AS 
            WITH normalized_spain_cl_player_names AS (
                SELECT  
                    "team_api_id",
                    "team_fifa_api_id",
                    TRANSLATE(
                        TRIM(
                            UPPER("scoring_player")
                        ),
                        'ÁÉÍÓÚáéíóúÑñ', 
                        'AEIOUaeiouNn'        
                    )AS simplified_player_name
                FROM STAGING.SPAIN.CHAMPIONS_LEAGUE_MATCHES
            ),
            normalized_spain_player_names AS (
                SELECT 
                    --"player_api_id",
                    *,
                    TRANSLATE(
                        TRIM(
                            UPPER("player_name")
                        ),
                        'ÁÉÍÓÚáéíóúÑñ', 
                        'AEIOUaeiouNn'        
                    )AS simplified_player_name
                FROM STAGING.SPAIN.PLAYER
            )
            select distinct CL_PLAYER.* EXCLUDE (simplified_player_name), SP_PLAYER.* EXCLUDE (simplified_player_name)
            from normalized_spain_cl_player_names CL_PLAYER
            join normalized_spain_player_names SP_PLAYER
            ON CL_PLAYER.simplified_player_name = SP_PLAYER.simplified_player_name;                
    """
    return query

def generate_query_consolidation(**kwargs):
    query = f"""
        DROP DATABASE IF EXISTS STAGING;
        
        CREATE DATABASE STAGING;
        
        USE STAGING;
        
        CREATE SCHEMA STAGING.SPAIN;
        
        {consolidation_query_european_soccer_db()}
        
        {consolidation_query_fifa_22_players_dataset()}
        
        {consolidation_query_football_data()}
    """
    return query


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["soccer_predictor"],
)

def soccer_predictor():

    with TaskGroup("download_sources") as download_sources:
        download_european_soccer_db = PythonOperator(
            task_id="download_european_soccer_db",
            python_callable=get_data_sources,
            op_kwargs={
                "kaggle_data_source": KAGGLE_DATASET_EUROPEAN_SOCCER_DATABASE,
                "folder_location": EUROPEAN_SOCCER_DB_FOLDER_NAME,
            },
        )

        download_fifa_22_players_task = PythonOperator(
            task_id="download_fifa_22_players_task",
            python_callable=get_data_sources,
            op_kwargs={
                "kaggle_data_source": KAGGLE_DATASET_FIFA_22_PLAYERS,
                "folder_location": FIFA_22_PLAYERS_DATASET_FOLDER_NAME,
            },
        )

        download_football_data_github_task = PythonOperator(
            task_id="download_football_data_from_github",
            python_callable=get_data_source_github,
            # do_xcom_push=True,  # Ensure the return value is pushed to XCom
        )

        convert_sqlite_to_csv = PythonOperator(
            task_id='convert_sqlite_to_csv',
            python_callable=sqlite_to_csv,
        )

        download_european_soccer_db >> convert_sqlite_to_csv


    with TaskGroup("upload_to_s3") as upload_to_s3:
        upload_files_european_soccer_db = PythonOperator(
            task_id="upload_files_european_soccer_db",
            python_callable=upload_files_to_s3,
            op_kwargs={
                "local_files_folder": EUROPEAN_SOCCER_DB_FOLDER_NAME,
                "s3_folder_name": EUROPEAN_SOCCER_DB_FOLDER_NAME,
            },
        )

        upload_files_fifa_22_players = PythonOperator(
            task_id="upload_files_fifa_22_players",
            python_callable=upload_files_to_s3,
            op_kwargs={
                "local_files_folder": FIFA_22_PLAYERS_DATASET_FOLDER_NAME,
                "s3_folder_name": FIFA_22_PLAYERS_DATASET_FOLDER_NAME,
            },
        )

        upload_files_football_data_task = PythonOperator(
            task_id="upload_files_football_data_task",
            python_callable=upload_files_to_s3,
            op_kwargs={
                "local_files_folder": FOOTBALL_DATA_FOLDER_NAME,
                "s3_folder_name": FOOTBALL_DATA_FOLDER_NAME,
            },
        )

    with TaskGroup("preparation_to_snowflake") as preparation_to_snowflake:
        generate_query_for_football_data_task = PythonOperator(
            task_id="generate_query_for_football_data_task",
            python_callable=generate_query_for_football_data,
        )

    with TaskGroup("snowflake_setup") as snowflake_setup:
        setup_config_on_snowflake = SQLExecuteQueryOperator(
            task_id="setup_config_on_snowflake",
            conn_id="snowflake_conn",
            sql=query_setup_snowflake(),
        )

    with TaskGroup("load_data_on_snowflake") as load_data_on_snowflake:
        load_european_soccer_db_on_snowflake = SQLExecuteQueryOperator(
            task_id="load_european_soccer_db_on_snowflake",
            conn_id="snowflake_conn",
            sql=get_table_creation_queries(
                snow_schema_name="EUROPEAN_SOCCER_DB",
                lst_tables=[
                    "Country",
                    "League",
                    "Match",
                    "Player",
                    "Player_Attributes",
                    "Team",
                    "Team_Attributes",
                ],
                topic=EUROPEAN_SOCCER_DB_FOLDER_NAME,
            ),
        )

        load_fifa_22_players_on_snowflake = SQLExecuteQueryOperator(
            task_id="load_fifa_22_players_on_snowflake",
            conn_id="snowflake_conn",
            sql=get_table_creation_queries(
                snow_schema_name="FIFA_22_PLAYERS_DATASET",
                lst_tables=[
                    "players_15",
                    "players_16",
                    "players_17",
                    "players_18",
                    "players_19",
                    "players_20",
                    "players_21",
                    "players_22",
                ],
                topic=FIFA_22_PLAYERS_DATASET_FOLDER_NAME,
            ),
        )

        load_football_data_on_snowflake = SQLExecuteQueryOperator(
            task_id="load_football_data_on_snowflake",
            conn_id="snowflake_conn",
            sql="{{ ti.xcom_pull(task_ids='preparation_to_snowflake.generate_query_for_football_data_task', key='sql_queries') }}",
        )

        load_parquet_football_data_on_snowflake = SQLExecuteQueryOperator(
            task_id="load_parquet_football_data_on_snowflake",
            conn_id="snowflake_conn",
            sql=get_query_load_parquet_file(
                schema_name="FOOTBALL_DATA", file_name="games.parquet"
            ),
        )

        load_football_data_on_snowflake >> load_parquet_football_data_on_snowflake

    with TaskGroup("consolidation") as consolidation:
        spain_consolidation_staging = SQLExecuteQueryOperator(
            task_id="spain_consolidation_staging",
            conn_id="snowflake_conn",
            sql=generate_query_consolidation
        )

    # Set dependencies between TaskGroups
    #download_sources >> upload_to_s3 >> preparation_to_snowflake >> snowflake_setup >> load_data_on_snowflake >> consolidation
    download_sources >> upload_to_s3 >> preparation_to_snowflake
    preparation_to_snowflake >> load_data_on_snowflake
    snowflake_setup >> load_data_on_snowflake >> consolidation

soccer_predictor()