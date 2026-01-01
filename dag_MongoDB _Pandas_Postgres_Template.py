# ----------------------------------
# Main Issue:
# Handling invalid UTF-8 byte sequences from MongoDB when transferring data to Postgres using Airflow DAG.
# ----------------------------------


"""
DAG Idea:
Task 1: Connect & extract only required cols data from MongoDB.
Task 2: Transform data using Pandas.
Task 3: Load data into Postgres.
"""

"""
DISCLAIMER:
This DAG is a personal demonstration project.
All database names, collection names, schemas, and fields are anonymized.
No proprietary or production data is included.
"""

from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine, text
import json
from bson import ObjectId
from airflow import DAG
import os
from dotenv import load_dotenv      
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

# Load environment variables from .env file
load_dotenv(dotenv_path="/opt/airflow/.env")  # adjust path if needed

# MongoDB connection
MONGO_URI = os.getenv("mongodb_conn")
MONGO_DB = "test_db"
MONGO_COLLECTION = "service"

#Postgres connection
PG_USER = os.getenv("Pg_User")
PG_PASSWORD = os.getenv("Pg_Password")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = "5432"
PG_DB = "db_test"

PG_CONN_STR = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"


# ------------------------
# Helper function (REQUIRED)
# ------------------------
# the UTF‑8 issue is that raw bytes may contain invalid sequences that can’t be decoded (e.g.xffworld)
# In Python, a value of type bytes is just raw binary data.
# If you want to treat those bytes as text (a string), you need to decode them into Unicode using a character encoding (like UTF-8).
# If the bytes contain invalid sequences for the chosen encoding, a UnicodeDecodeError will be raised.

def clean_value(x):
    # If x is None, it returns None instead of crashing or returning something ambiguous.
    if x is None:
        return None
    if isinstance(x, bytes):
        # Fix invalid UTF-8 bytes to valid UTF-8
        return x.decode("utf-8", errors="ignore") #  With errors="ignore" → invalid byte skipped
    if isinstance(x, ObjectId):
        return str(x)
    return x


 
# ------------------------
# Extract Function
# ------------------------
def extract_from_mongo(**context):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    # Fetch all documents
    data = list(collection.find())
    df = pd.DataFrame(data)

    # 🔑 CRITICAL FIX: clean bytes & ObjectId BEFORE to_json
    df = df.applymap(clean_value)

    print(f"Extracted {len(df)} records from MongoDB.")

    # Push dataframe to XCom as JSON string
    context["ti"].xcom_push(
        key="raw_data",
        value=df.to_json(orient="records", force_ascii=False)
    )


# ------------------------
# Transform Function
# ------------------------
def transform_data(**context):
    # Pull raw JSON from XCom
    raw_json = context["ti"].xcom_pull(task_ids="extract_data", key="raw_data")
    df = pd.DataFrame(json.loads(raw_json))

    # Normalize column names
    df.columns = [col.lower() for col in df.columns]
    df.rename(columns={"desc": "description"}, inplace=True)

    # Clean values
    def clean_value(x):
        if isinstance(x, ObjectId):
            return str(x)
        return x

    df = df.applymap(clean_value)
    df = df.applymap(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    # Select required columns
    required_columns = [
        "_id", "actionpriority", "activateby", 
        "category", "channels", "createdat", "description",
        "isrefund", "name","status", "updatedat", "updatedby"
    ]
    df = df[[col for col in required_columns if col in df.columns]]

    # Push cleaned data to XCom
    context["ti"].xcom_push(key="clean_data", value=df.to_json(orient="records"))


# ------------------------
# Load Function
# ------------------------
def load_to_postgres(**context):
    clean_json = context["ti"].xcom_pull(task_ids="transform_data", key="clean_data")
    df = pd.DataFrame(json.loads(clean_json))

    # by using SQLAlchemy engine, it manages connections efficiently (i.e., connection,cursor, etc.)
    engine = create_engine(PG_CONN_STR)
    with engine.connect() as connection:
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
            updatedat TIMESTAMP,
            updatedby JSONB
        );
        """
        connection.execute(text(create_table_query))

        df.to_sql("service", con=connection, if_exists="replace", index=False)
        print(" Data loaded into Postgres table: service")


# ------------------------
# DAG Definition
# ------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 15),
    "retries": 0,
}

with DAG(
    dag_id="etl_mongo_to_postgres",
    default_args=default_args,
    schedule_interval=None, # as one time run for full data load
    catchup=False, # do not perform backfill
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

    extract_task >> transform_task >> load_task
