# ---------------------------
# Main Issue
# -------------------------
# The main issue in this exercise is datetime cols issue when loading data from MongoDB to PostgreSQL.
# MongoDB datetime fields may contain null values, which are represented as NaT in Pandas.
# PostgreSQL does not accept NaT values, leading to errors during data insertion.
# The solution involves converting NaT values to None before loading data into PostgreSQL.

"""
DAG Idea:
Task 1: Connect & extract only required cols data from MongoDB.
Task 2: Transform data using Pandas.
Task 3: Load data into Postgres.
Task 4: Reconciliation check between MongoDB and Postgres.
"""

"""
DISCLAIMER:
This DAG is a personal demonstration project.
All database names, collection names, schemas, and fields are anonymized.
No proprietary or production data is included.
"""


from matplotlib.style import context
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
import pandas as pd
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime

# --------------------------------------------------
# Load ENV variables
# --------------------------------------------------
load_dotenv()

MONGO_URI = os.getenv("mongodb_conn")
MONGO_DB = "test_db"
MONGO_COLLECTION = "customer"

PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = "5432"
PG_DB = "db_test"

# --------------------------------------------------
# Helper Functions
# --------------------------------------------------
def get_pg_conn():
    return psycopg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        host=PG_HOST,
        port=PG_PORT
    )

def normalize_columns(df):
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    return df

def clean_value(x):
    if isinstance(x, dict) or isinstance(x, list):
        return json.dumps(x, default=str)
    return x

# --------------------------------------------------
# Task 1: Check Connections
# --------------------------------------------------
def check_connections():
    # Mongo
    mongo_client = MongoClient(MONGO_URI)
    mongo_client.admin.command("ping")

    # Postgres
    conn = get_pg_conn()
    conn.cursor().execute("SELECT 1")
    conn.close()

    print(" MongoDB and PostgreSQL connections successful")

# --------------------------------------------------
# Task 2: Extract Mongo → Postgres (Staging)
# --------------------------------------------------
def extract_mongo_to_staging(**kwargs): #**kwars means accept any number of keyword arguments.
    ti = kwargs["ti"] # Task Instance for XComs
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]

    selected_columns = {
        "_id": 1,
        "address": 1,
        "birth": 1,
        "channel": 1,
        "createdAt": 1,
        "email": 1,
        "gender": 1,
        "cusInfo": 1,
        "name": 1,
        "nid": 1,
        "phone": 1,
        "status": 1,
        "updatedAt": 1
    }

    created_from = datetime(2025, 12, 1)

    query = {
    "createdAt": {"$gte": created_from}
      }

    docs = list(collection.find(query, selected_columns)) # query mongo data
    
    
    df = pd.DataFrame(docs)

    # Transformations
    df["_id"] = df["_id"].astype(str) # Convert ObjectId to str to be compactible with Postgres.
    df = normalize_columns(df) # Normalize column names to lowercase.

    # Convert nested objects to JSON strings to be Postgres-safe
    for col in ["mmqrinfo", "nric"]:
        if col in df.columns:
            df[col] = df[col].apply(clean_value)

    # Handle datetime columns - Convert to Pandas datetime
    datetime_cols = [
        "createdat", "updatedat", "birth"
    ]
    for col in datetime_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    
    # errors="coerce": invalid parsing will be set as NaT
    #  CRITICAL FIX: Convert NaT / NaN → None (Postgres-safe)
    # Force Pandas to release datetime64 dtype and convert NaT → None
    df = df.astype(object).where(pd.notnull(df), None)
    

    # Load into Postgres staging as temporary table for large volume insert
    conn = get_pg_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS staging.customer_raw_v2 (
            _id TEXT PRIMARY KEY,
            address TEXT,
            birth TIMESTAMP,
            channel TEXT,
            createdat TIMESTAMP,
            email TEXT,
            gender TEXT,
            curinfo JSONB,
            name TEXT,
            nid JSONB,
            phone TEXT,
            status TEXT,
            updatedat TIMESTAMP
        )
    """)

    cols = list(df.columns)
    # Pandas Index object containing the column names of  DataFrame. Index(['Name', 'Age', 'Salary'], dtype='object')
    values = [tuple(row) for row in df.to_numpy()] 
    # df.to_numpy() : converts the DataFrame into a 2D NumPy array, shape (num_rows, num_columns).
    # Iterates row by row, each row is a NumPy array.
    # tuple(row): converts each row from NumPy array -> Python tuple, because execute_values() expects a list of tuples.

    insert_sql = f"""
        INSERT INTO staging.customer_raw_v2 ({','.join(cols)})
        VALUES %s
        ON CONFLICT (_id) DO NOTHING
    """

    execute_values(cursor, insert_sql, values) # execute_values() expects a list of tuples.
    conn.commit()
    cursor.close()
    conn.close()

    # ADDED: push Mongo row count to XCom for reconciliation
    ti.xcom_push(
        key="mongo_extracted_count",
        value=len(df)
    )

    print(" MongoDB data loaded into staging.customer_raw")

# --------------------------------------------------
# Task 3: Transform & Load to Final Data Mart
# --------------------------------------------------
def transform_staging_to_final():
    conn = get_pg_conn() # returns a PostgreSQL connection object
    cursor = conn.cursor() # returns a cursor object to execute SQL commands

    # --------------------------------------------------
    # Create Final Table (All cols + flattened fields)
    # --------------------------------------------------
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.customer_v2 (
            _id TEXT PRIMARY KEY,
            address TEXT,
            birth TIMESTAMP,
            channel TEXT,
            createdat TIMESTAMP,
            email TEXT,
            gender TEXT,
            curinfo JSONB,
            name TEXT,
            nid JSONB,
            phone TEXT,
            status TEXT,
            updatedat TIMESTAMP,

            -- Flattened CURINFO fields
            curinfo_account_number TEXT,
            curinfo_status TEXT,
            -- Flattened NID fields
            nid_code INTEGER,
            nid_name TEXT,
            nid_type TEXT,
            nid_number TEXT
        )
    """)

    # --------------------------------------------------
    # Insert with JSON flattening (NULL safe)
    # --------------------------------------------------
    cursor.execute("""
        INSERT INTO public.customer_v2 (
            _id TEXT PRIMARY KEY,
            address TEXT,
            birth TIMESTAMP,
            channel TEXT,
            createdat TIMESTAMP,
            email TEXT,
            gender TEXT,
            curinfo JSONB,
            name TEXT,
            nid JSONB,
            phone TEXT,
            status TEXT,
            updatedat TIMESTAMP,

          
            curinfo_account_number TEXT,
            curinfo_status TEXT,
           
            nid_code INTEGER,
            nid_name TEXT,
            nid_type TEXT,
            nid_number TEXT
        )
        SELECT
           _id TEXT PRIMARY KEY,
            address TEXT,
            birth TIMESTAMP,
            channel TEXT,
            createdat TIMESTAMP,
            email TEXT,
            gender TEXT,
            curinfo JSONB,
            name TEXT,
            nid JSONB,
            phone TEXT,
            status TEXT,
            updatedat TIMESTAMP,

            -- cusInfoflatten (NULL safe)
            curinfo ->> 'accountNumber',
            curinfo ->> 'status',
            
            -- NRIC flatten (NULL safe)
            -- cast to INTEGER [Now '123' (TEXT) → 123 (INTEGER)]
            (nid ->> 'code')::INTEGER,
            nid ->> 'name',
            nid ->> 'type',
            nid ->> 'number'
        FROM staging.customer_raw_v2
        ON CONFLICT (_id) DO NOTHING
    """)

    conn.commit() # saves changes permanently to PostgreSQL.
    cursor.close() # release resources.
    conn.close() # release resources.

    print(" All columns loaded + mmqrInfo & nric flattened successfully")
    
    
#---------------------------------------------------------------------------
# Task 4: Reconciliation Check (Mongo → Staging & Staging → Final)
#---------------------------------------------------------------------------
def reconcile_mongo_vs_postgres(**kwargs):
    ti = kwargs["ti"]

    # Step 1: Mongo → Stagin
    mongo_count = ti.xcom_pull(
        task_ids="extract_mongo_to_staging",
        key="mongo_extracted_count"
    )

    conn = get_pg_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM staging.customer_raw_v2")
    staging_count = cursor.fetchone()[0]


    if mongo_count != staging_count:
        raise ValueError(
            f"CHECK FAILED | Mongo: {mongo_count}, Postgres: {staging_count}"
        )

    print(
        f"CHECK PASSED | Mongo: {mongo_count}, Postgres: {staging_count}"
    )


    # Step 2: Staging → Final
    cursor.execute("SELECT COUNT(*) FROM public.customer_v2")
    final_count = cursor.fetchone()[0]

    if staging_count != final_count:
        raise ValueError(
            f"CHECK FAILED | Staging → Final | Staging: {staging_count}, Final: {final_count}"
        )
    else:
        print(f"CHECK PASSED | Staging → Final | Staging: {staging_count}, Final: {final_count}")

    cursor.close()
    conn.close()
# --------------------------------------------------
# DAG Definition
# --------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 0
}

with DAG(
    dag_id="mongo_to_postgres_staging__Reconciliation_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["mongodb", "postgres", "etl"]
) as dag:

    check_conn = PythonOperator(
        task_id="check_connections",
        python_callable=check_connections
    )

    extract_task = PythonOperator(
        task_id="extract_mongo_to_staging",
        python_callable=extract_mongo_to_staging
    )

    transform_task = PythonOperator(
        task_id="transform_staging_to_final",
        python_callable=transform_staging_to_final
    )
    
    reconcile_task = PythonOperator(
        task_id="reconcile_mongo_vs_postgres",
        python_callable=reconcile_mongo_vs_postgres
        
    )

    check_conn >> extract_task >> transform_task >> reconcile_task
