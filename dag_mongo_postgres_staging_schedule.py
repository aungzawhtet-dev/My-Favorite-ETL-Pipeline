# ---------------------------
# Main Issue
# -------------------------
# The main issue in this exercise is to start run the script from last_final_ts 
# find the max createdat from the final table public.customer_v2 in Postgres
# and use that timestamp to extract only new or updated records from MongoDB
# handle edge cases like if can't get max createdat (i.e., no rows yet),start from DAG start_date
# and ensure the extraction query correctly fetches records with createdAt greater than last_final_ts   
# best practices first backfill the data from the start date to now
# that means if there is gap between max date in the table and DAG start date

"""
DISCLAIMER:
This DAG is a personal demonstration project.
All database names, collection names, schemas, and fields are anonymized.
No proprietary or production data is included.
"""


"""
DAG Idea:
# Connect & extract only required cols data from MongoDB.
# Transform data using Pandas & insert into Postgres statging.
# flat data as per business requirements & Load data into Postgres data mart.
"""


from datetime import timedelta, timezone
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

# --------------------------------------------------
# Load ENV variables
# --------------------------------------------------
load_dotenv()

MONGO_URI = os.getenv("mongodb_conn")
MONGO_COLLECTION = "customer"
MONGO_DB = "db_test"

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
    # If x is None, it returns None instead of crashing or returning something ambiguous.
    if x is None:
        return None
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
# **kwargs allows a function to accept any number of keyword arguments as a dictionary
# **context is just a naming choice — it works the same as **kwargs
def extract_mongo_to_staging(**context):
    client = MongoClient(MONGO_URI)
    
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]
        

        selected_columns = {
            "_id": 1,
            "address": 1,
            "birth": 1,
            "channel": 1,
            "code": 1,
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
        

        # Incremental Logic using Airflow context
        # -------------------------------
        # Get max(createdat) from final table
        # -------------------------------
        conn = get_pg_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(createdat) FROM public.customer_v2")
        last_final_ts = cursor.fetchone()[0] # Last final timestamp Max(createdat)
        cursor.close()
        conn.close()

        # If no rows yet, start from DAG start_date
        if last_final_ts is None:
            last_final_ts = datetime(2025, 12, 31, 0, 0, tzinfo=timezone.utc) # DAG start_date

        print(f"Extracting Mongo data from {last_final_ts} → now")

        # Query MongoDB for new/updated records
        # This is a Python dictionary that represents a MongoDB query filter.
        query = {
            "createdAt": {
                "$gte": last_final_ts - timedelta(seconds=1),  # 1-second overlap to secure no gap
            }
        }

        
        # (1 = ascending, -1 = descending)
        docs = list(collection.find(query, selected_columns).sort([("createdAt", 1), ("_id", 1)]))
        df = pd.DataFrame(docs)

        # Handle empty DataFrame
        if df.empty:
            print("No new records found for this extraction.")
            context["ti"].xcom_push(key="has_data", value=False)
            return

        context["ti"].xcom_push(key="has_data", value=True)
        # key="has_data", value=False --> This task found no data.
        

        # Transformations
        df["_id"] = df["_id"].astype(str) # Convert ObjectId to str to be compactible with Postgres.
        df = normalize_columns(df) # Normalize Mongo column names to lowercase.

        # Convert nested objects/Documents to JSON strings to be Postgres-safe
        for col in ["mmqrinfo", "nric"]:
            if col in df.columns:
                df[col] = df[col].apply(clean_value)

        # Handle datetime columns - Convert to Pandas datetime
        datetime_cols = [
            "createdat", "updatedat", "birth"
        ]
        # If a value cannot be parsed as a datetime (e.g., invalid string),
        # it will be replaced with NaT (Not a Time), Pandas’ special null value for datetime.
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        
        # CRITICAL FIX: Convert NaT / NaN → None (Postgres-safe)
        # Force Pandas to release datetime64 dtype and convert NaT → None
        # PostgreSQL adapters expect Python-native types (datetime.datetime, None) rather than Pandas-specific ones (NaT, numpy.datetime64).
        df = df.astype(object).where(pd.notnull(df), None)
        
    finally:
        client.close()
        print("MongoDB client closed")
        
    
    # Load into Postgres staging for large volume data insert
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
            cusinfo JSONB,
            name TEXT,
            nid JSONB,
            phone TEXT,
            status TEXT,
            updatedat TIMESTAMP
        )
    """)

    cols = list(df.columns)
    # Pandas Index object containing the column names of  DataFrame --> Index(['id', 'name', 'createdat'], dtype='object')
    values = [tuple(row) for row in df.to_numpy()] 
    # df.to_numpy() : converts the DataFrame into a 2D NumPy array, shape (num_rows, num_columns).
    # Iterates row by row, each row is a NumPy array.
    # tuple(row): converts each row from NumPy array -> Python tuple, because execute_values() expects a list of tuples.

    insert_sql = f"""
        INSERT INTO staging.customer_raw_v2 ({','.join(cols)})
        VALUES %s
        ON CONFLICT (_id) DO NOTHING
    """

    # execute_values() expects a list of tuples with error handling
    try:
        execute_values(cursor, insert_sql, values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    print(f"Inserted {len(df)} rows")

# --------------------------------------------------
# Task 3: Transform from staging to Load to Final Data Mart
# --------------------------------------------------
def transform_staging_to_final(**context):
    has_data = context["ti"].xcom_pull(
        task_ids="extract_mongo_to_staging",
        key="has_data"
    )

    if not has_data:
        print("Skipping transform — no new data.")
        return
    
    
    
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
            cusinfo JSONB,
            name TEXT,
            nid JSONB,
            phone TEXT,
            status TEXT,
            updatedat TIMESTAMP,

            -- Flattened Cusinfo fields
            cusinfo_account_number TEXT,
            cusinfo_status TEXT,

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
            _id,
            address,
            birth,
            channel,
            createdat,
            email,
            gender,
            cusinfo,
            name,
            nid,
            phone,
            status,
            updatedat,

            cusinfo_account_number,
            cusinfo_status,

            nid_code,
            nid_name,
            nid_type,
            nid_number
        )
        SELECT
            _id,
            address,
            birth,
            channel,
            createdat,
            email,
            gender,
            cusinfo,
            name,
            nid,
            phone,
            status,
            updatedat,

            -- Cusinfo flatten (NULL safe)
            cusinfo ->> 'accountNumber',
            cusinfo ->> 'status',

            -- NID flatten (NULL safe)
            -- cast to INTEGER [Now '123' (TEXT) → 123 (INTEGER)]
            (nid ->> 'code')::INTEGER,
            nid ->> 'name',
            nid ->> 'type',
            nid ->> 'number'
        FROM staging.customer_raw_v2
        ON CONFLICT (_id) DO NOTHING
        """)
    
    
    # Delete processed rows from staging
    cursor.execute("TRUNCATE staging.customer_raw_v2")
    conn.commit()
    cursor.close()
    conn.close()

    print("All staging rows loaded into final + staging cleared")

# --------------------------------------------------
# DAG Definition
# --------------------------------------------------
default_args = {
    "owner": "azh",
    "depends_on_past": False,           # don't wait for previous run
    "email": ["alert@company.com"],     # monitoring alerts
    "email_on_failure": True,           # notify on failure
    "email_on_retry": False,            # avoid spam on retries
    "retries": 3,                       # retry up to 3 times
    "retry_delay": timedelta(minutes=10),   # wait 10 minutes between retries   
    "execution_timeout": timedelta(minutes=120), # prevent stuck tasks. If a task runs longer than 120 minutes, it will be terminated. 
}

with DAG(
    dag_id="mongo_to_postgres_staging_schedule",
    start_date=datetime(2025, 12, 31, 0, 0, tzinfo=timezone.utc),
    schedule_interval="*/30 * * * *",   # every 30 minutes
    catchup=True,                       # IMPORTANT for incremental correctness
    default_args=default_args,
    tags=["mongodb", "postgres", "etl"]
) as dag:

    check_conn = PythonOperator(
        task_id="check_connections",
        python_callable=check_connections
    )

    extract_task = PythonOperator(
        task_id="extract_mongo_to_staging",
        python_callable=extract_mongo_to_staging,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id="transform_staging_to_final",
        python_callable=transform_staging_to_final
    )

    check_conn >> extract_task >> transform_task
