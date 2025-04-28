from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from sqlLite_DB_creation_script import create_tables_from_csv

from airflow.operators.bash import BashOperator
import shutil
import sqlite3
import os
import pandas as pd
import duckdb
import logging

# Filepaths
SQLITE_DB_PATH = '/mnt/e/Nauka/DE_Bootcamp/Projects/DBT_project1_ecommerce/pythonProject/ecommerce.db'
SQLITE_SNAPSHOT_DIR_PATH = '/mnt/e/Nauka/DE_Bootcamp/Projects/DBT_project1_ecommerce/pythonProject/SQLiteSnapshots'
CSV_FOLDER = "/mnt/e/Nauka/DE_Bootcamp/Projects/DBT_project1_ecommerce/archive"
DUCKDB_PATH = "/mnt/e/Nauka/DE_Bootcamp/Projects/DBT_project1_ecommerce/ecommerce_project.duckdb"
DBT_PROJECT_PATH = "/mnt/e/Nauka/DE_Bootcamp/Projects/DBT_project1_ecommerce/dbtProject/ecommerce_project"
DBT_PROFILES_DIR = "/mnt/c/Users/[USER_NAME]/.dbt"

# Tasks

def create_sqlite_snapshot():
    """
    Creates a snapshot of the SQLite DB and saves it to the snapshot directory.
    Input: none
    Output: none
    """

    # make sure that the snapshot dir exists, if not than create it
    os.makedirs(SQLITE_SNAPSHOT_DIR_PATH, exist_ok=True)

    # get the current time in format YYYMMDD_HHMMSS
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # create the full file path of the snapshot file
    snapshot_path = os.path.join(SQLITE_SNAPSHOT_DIR_PATH, f'mydata_snapshot_{timestamp}.db')

    # create snapshot and safe it to the snapshot dir
    shutil.copy2(SQLITE_DB_PATH, snapshot_path)

    logging.info(f"Snapshot created at {SQLITE_SNAPSHOT_DIR_PATH}")


def delete_all_sqlite_tables():
    """
    Deletes all existing tables in the SQLite DB.
    This step is required to recreate them in next step from the CSV-Files
    Input: none
    Output: none
    """
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    logging.info(f"All tables: {tables}")

    for (table,) in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")
        logging.info(f"Dropped table: {table}")

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    logging.info(f"All tables: {tables}")

    conn.commit()
    conn.close()


def recreate_tables_from_csv():

    """
    Recreates tables in the SQLite DB from the CSV-Files.
    INPUT: none
    Output: none

    """
    create_tables_from_csv(SQLITE_DB_PATH, CSV_FOLDER)
    logging.info(f"Recreated all tables in SQLite DB from CSV Files")


def load_sqlite_to_duckdb():
    duck = duckdb.connect(DUCKDB_PATH)
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in sqlite_cursor.fetchall()]

    # Drop existing tables in DuckDB
    for table in tables:
        duck.execute(f"DROP TABLE IF EXISTS {table};")
        logging.info(f"Dropped table: {table}")

    # Load SQLite tables into DuckDB
    for table in tables:
        df = pd.read_sql_query(f"SELECT * FROM {table}", sqlite_conn)
        duck.register('df_view', df)  # Register DataFrame in DuckDB
        duck.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM df_view")
        print(f"Loaded table '{table}' into DuckDB")

    duck.close()
    sqlite_conn.close()


# DAG Definition

with DAG(
        dag_id="sqlite_to_duckdb_pipeline",
        description="Pipeline to snapshot SQLite, rebuild from CSV, and load into DuckDB with DBT",
        schedule_interval='*/30 * * * *',
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["sqlite", "duckdb", "dbt", "DEProject"],
) as dag:
    snapshot_sqlite = PythonOperator(
        task_id="snapshot_sqlite_db",
        python_callable=create_sqlite_snapshot,
    )

    delete_sqlite_tables = PythonOperator(
        task_id="delete_sqlite_tables",
        python_callable=delete_all_sqlite_tables,
    )

    recreate_sqlite_tables = PythonOperator(
        task_id="recreate_sqlite_tables_from_csv",
        python_callable=recreate_tables_from_csv,
    )

    load_to_duckdb = PythonOperator(
        task_id="load_sqlite_to_duckdb",
        python_callable=load_sqlite_to_duckdb,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_PATH} && /home/cvieb/airflow/venv/bin/dbt seed --profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_PATH} && /home/cvieb/airflow/venv/bin/dbt run --profiles-dir {DBT_PROFILES_DIR}",
    )

    # DAG Flow
    snapshot_sqlite >> delete_sqlite_tables >> recreate_sqlite_tables
    recreate_sqlite_tables >> load_to_duckdb >> dbt_seed >> dbt_run
