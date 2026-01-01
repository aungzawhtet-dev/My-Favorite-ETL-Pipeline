# ----------------------------------
# Main Issue:
# (1) Handling invalid UTF-8 byte sequences from MongoDB when transferring data to Postgres using Airflow DAG.
# (2) Overcoming Airflow XCom size limits by using temporary JSON file for data transfer between tasks.
# fixed the datetime in Pandas memory that does NOT guarantee what happens when you save to JSON.
# default dangerous behavior of pandas.to_json is to convert datetime to epoch milliseconds(int).
# if without define date_format="iso", date_unit="ms", the datetime will be saved as integer ms in JSON file.
# ----------------------------------


"""
DAG Idea:
# Connect & extract only required cols data from MongoDB.
# Extracted data is passed via temporary JSON file to avoid XCom size limits.
# Transform data using Pandas.
# Load data into Postgres.
# Perform data quality checks using Soda scan subprocess.
"""

"""
DISCLAIMER:
This DAG is a personal demonstration project.
All database names, collection names, schemas, and fields are anonymized.
No proprietary or production data is included.
"""


import logging
import os
import json
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
import pandas as pd
from sqlalchemy import create_engine, TIMESTAMP, text
from sqlalchemy.dialects.postgresql import JSONB
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from airflow.exceptions import AirflowException
import subprocess

# Load environment variables
load_dotenv(dotenv_path="/opt/airflow/.env")  # adjust path if needed

# MongoDB
MONGO_URI = os.getenv("mongodb_conn")
MONGO_DB = "test_db"
MONGO_COLLECTION = "service"

# Postgres
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = "5432"
PG_DB = "db_test"
PG_CONN_STR = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# Temp JSON file path
# In containerized environments (like Docker/Kubernetes), /tmp exists inside the container’s filesystem, not on your host machine.
TEMP_JSON_PATH = "/tmp/service_data.json"


# ------------------------
# Helper function
# ------------------------
def clean_value(x):
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="ignore")
    if isinstance(x, ObjectId):
        return str(x)
    return x


# ------------------------
# Extract Task
# ------------------------
def extract_from_mongo():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    data = list(collection.find()) # fetch all documents
    df = pd.DataFrame(data)
    
    # MongoDB _id looks like ObjectId ("656ab23cfa9a0d6b45f8e9a1") BSON data type
    # convert to string as Postgres cannot handle ObjectId type
    # convert Mongo ObjectId to string
    df = df.applymap(clean_value)

    # Save to temp JSON file instead of XCom
    # to overcome xcom size limits
    df.to_json(TEMP_JSON_PATH, orient="records", force_ascii=False)
    logging.info(f"Extracted {len(df)} records from MongoDB into {TEMP_JSON_PATH}")


# ------------------------
# Transform Task
# ------------------------
def transform_data():
    df = pd.read_json(TEMP_JSON_PATH)

    # Normalize columns (names to lowercase, rename desc to description)
    df.columns = [col.lower() for col in df.columns]
    df.rename(columns={"desc": "description"}, inplace=True)

    # Convert dates from MongoDB to Postgres format (TIMESTAMP)
    # MongoDB date representations: 1.BSON Date, 2.Epoch milliseconds, 3.String (bad but common)
    # 1.{ "createdAt": ISODate("2025-01-01T10:00:00Z") }, 2.{ "createdAt": 1735725600000 }, 3.{ "createdAt": "2025-01-01T10:00:00Z" }
    
    for col in ["createdat", "updatedat"]:
        if col in df.columns:
            # from epoch milliseconds -> datetime
            df[col] = pd.to_datetime(df[col], errors="coerce", unit="ms")

    # Convert nested dict/list to JSON strings (kept as in your original)
    # MongoDB allows nested structures; PostgreSQL does not (unless you explicitly use JSON/JSONB)
    # When loaded into Pandas:dict → Python dict / array → Python list
    # Scan every cell in the DataFrame values (dict/list → JSON string) as PostgreSQL can store this as TEXT
    df = df.applymap(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    # Select required columns
    required_columns = [
        "_id", "actionpriority", "category", "channels", 
        "createdat", "description","name", "isrefund", "activateby",
        "status", "updatedat", "updatedby"
    ]
    df = df[[col for col in required_columns if col in df.columns]]


    # --------------------
    # DATETIME FIX :
    # Save datetimes as ISO strings so they don't turn back into epoch integers
    # --------------------
    # the following code solve two different failure points.
    # 1. fixed the datetime in Pandas memory that does NOT guarantee what happens when you save to JSON.
    # 2. default dangerous behavior of pandas.to_json is to convert datetime to epoch milliseconds(int).
    # if without define date_format="iso", date_unit="ms", the datetime will be saved as integer ms in JSON file.
    
    df.to_json(
        TEMP_JSON_PATH,
        orient="records",
        force_ascii=False,
        date_format="iso",   # <= key change
        date_unit="ms"       # preserve ms precision if present
    )
    logging.info(f"Transformed data saved to {TEMP_JSON_PATH}")


# ------------------------
# Load Task
# ------------------------
def load_to_postgres():
    df = pd.read_json(TEMP_JSON_PATH)

    # --------------------
    # DATETIME FIX:
    # Re-parse ISO strings to pandas datetime and remove timezone
    # to match TIMESTAMP WITHOUT TIME ZONE in Postgres
    # --------------------
    for col in ["createdat", "updatedat"]:
        if col in df.columns:
            # If ISO string -> parse; if (unexpected) integer ms -> handle too
            # errors="coerce" means:If a value cannot be converted to a datetime, replace it with NaT instead of raising an error.
            if pd.api.types.is_integer_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], errors="coerce", unit="ms")
            else:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            # make (no tz) for TIMESTAMP WITHOUT TIME ZONE for Postgres compatibility
            df[col] = df[col].dt.tz_localize(None)

    engine = create_engine(PG_CONN_STR)
    with engine.connect() as conn:
        # Create table if not exists
        create_table_query = """
        CREATE TABLE IF NOT EXISTS service (
            _id TEXT PRIMARY KEY,
            actionpriority TEXT,
            activateby JSONB,
            category TEXT,
            channels JSONB,
            createdat TIMESTAMP,
            description TEXT,
            isrefund TEXT,
            name TEXT,
            status TEXT,
            updatedat TIMESTAMP
        );
        """
        conn.execute(text(create_table_query))

        # Load data
        df.to_sql(
            "service",
            con=conn,
            if_exists="replace",  # "append" avoids dropping table, "replace" for fresh load
            index=False,
            dtype={
                "channels": JSONB,
                "activateby": JSONB,
                "createdat": TIMESTAMP,
                "updatedat": TIMESTAMP,
            }
        )
    logging.info("Data loaded into Postgres table: service")


# ------------------------
# Soda Quality Check
# ------------------------
def run_soda_quality_check():
    # stdout (scan o/p)/ stderror (error o/p) --> (e.g. print())
    # logging logic is required in Soda scan subprocess as Airflow won't capture subprocess o/p automatically
    logging.info("Running Soda quality scan...")
    result = subprocess.run(
        ["soda", "scan", "-d", "postgres_db",
         "-c", "/opt/airflow/soda/configuration.yml",
         "/opt/airflow/soda/checks_service.yml"],
        capture_output=True,
        text=True
    )
    logging.info(result.stdout)
    logging.error(result.stderr)
    if result.returncode != 0:
        raise AirflowException("Data quality checks failed")
    logging.info("Data quality checks passed")


# ------------------------
# DAG Definition
# ------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 15),
    "retries": 0,
}

with DAG(
    dag_id="etl_mongo_to_postgres_soda_production",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["etl", "mongodb", "postgres"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_from_mongo,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_to_postgres,
    )

    quality_task = PythonOperator(
        task_id="soda_quality_check",
        python_callable=run_soda_quality_check,
    )

    extract_task >> transform_task >> load_task >> quality_task
