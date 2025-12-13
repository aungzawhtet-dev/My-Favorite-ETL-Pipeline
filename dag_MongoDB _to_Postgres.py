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
load_dotenv()   # expects .env configured in runtime environment

# MongoDB connection
MONGO_URI = os.getenv("mongodb_conn_new")
MONGO_DB = "TestDB"
MONGO_COLLECTION = "sample_collection"

# Postgres connection
PG_USER = os.getenv("Pg_User")
PG_PASSWORD = os.getenv("Pg_Password")
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "db_test"

PG_CONN_STR = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"



# Extract Function
# ------------------------
def extract_from_mongo(**context):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    # Fetch all documents
    data = list(collection.find()) # For demo purposes: full collection extraction
    df = pd.DataFrame(data)

    print(f"Extracted {len(df)} records from MongoDB.")

    # Push dataframe to XCom as JSON string
    context["ti"].xcom_push(key="raw_data", value=df.to_json(orient="records"))
    # NOTE: XCom used for demo; production pipelines would persist data in staging storage




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
    # Column names are anonymized placeholders for demonstration
    required_columns = [
        "_id", "action","activateby", 
         "category", "channels", "createdat", "description",
        "fullname", "name", "notes",
         "status", "updatedat", "updatedby"
    ]
    df = df[[col for col in required_columns if col in df.columns]]

    # Push cleaned data to XCom
    context["ti"].xcom_push(key="clean_data", value=df.to_json(orient="records"))
    # NOTE: XCom used for demo; production pipelines would persist data in staging storage




# Load Function
# ------------------------
def load_to_postgres(**context):
    clean_json = context["ti"].xcom_pull(task_ids="transform_data", key="clean_data")
    df = pd.DataFrame(json.loads(clean_json))

    engine = create_engine(PG_CONN_STR)
    with engine.connect() as connection:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Test_Table (
            _id TEXT PRIMARY KEY,
            action JSONB,
            activateby JSONB,
            category TEXT,
            channels JSONB,
            createdat TIMESTAMP,
            description TEXT,
            fullname TEXT,
            name TEXT,
            notes TEXT,
            status TEXT,
            updatedat TIMESTAMP,
            updatedby JSONB
        );
        """
        connection.execute(text(create_table_query))

        df.to_sql("Test_Table", con=connection, if_exists="replace", index=False)
        print(" Data loaded into Postgres table: Test_Table")



# DAG Definition
# ------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 15),
    "retries": 1,
}

with DAG(
    dag_id="etl_mongo_to_postgres_env",
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

    extract_task >> transform_task >> load_task
