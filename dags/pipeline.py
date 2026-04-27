import pandas as pd
from datetime import datetime
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from airflow import DAG
import json
import os

REQUIRED_COLUMNS = [
    "Ticket_ID",
    "Customer_Name",
    "Customer_Email",
    "Ticket_Subject",
    "Ticket_Description",
    "Issue_Category",
    "Priority_Level",
    "Ticket_Channel",
    "Submission_Date",
    "Resolution_Time_Hours",
    "Assigned_Agent",
    "Satisfaction_Score",
    "ticket_age",
    "performance",
]


CSV_PATH = "/opt/airflow/data/customer_support_tickets.csv"
CHUNK_SIZE = 100
OFFSET_FILE = "/opt/airflow/data/offset.json"


def load_offset() -> int:
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE) as f:
            return json.load(f)["offset"]
    return 0


def save_offset(offset: int):
    with open(OFFSET_FILE, "w") as f:
        json.dump({"offset": offset}, f)


def extract(**context):
    offset = load_offset()

    chunk = pd.read_csv(
        CSV_PATH,
        skiprows=range(1, offset + 1),
        nrows=CHUNK_SIZE,
        header=0,
    )

    if chunk.empty:
        print("No more rows to process.")
        return None

    save_offset(offset + len(chunk))
    print(f"Extracted rows {offset} → {offset + len(chunk)}")

    chunk_path = f"/tmp/chunk_{offset}.csv"
    chunk.to_csv(chunk_path, index=False)

    return chunk_path



def transform(file_path: str):
    if file_path is None:
        return None
    
    df = pd.read_csv(file_path)

    # remove index column
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    # drop duplicates
    df = df.drop_duplicates()

    if "Ticket_Subject" in df.columns:
        df["Ticket_Subject"] = df["Ticket_Subject"].str.split("-").str[0].str.strip()

    # clean Ticket_Description (remove "Hi Support")
    if "Ticket_Description" in df.columns:
        df["Ticket_Description"] = (
            df["Ticket_Description"]
            .str.replace("Hi Support,", "", regex=False)
            .str.replace("Hi Support", "", regex=False)
            .str.strip()
        )

    if "Submission_Date" in df.columns:
        df["Submission_Date"] = pd.to_datetime(df["Submission_Date"], errors="coerce")

    df["ticket_age"] = df["Submission_Date"].apply(
        lambda x: 
        "new" if pd.notnull(x) and x.year == 2025 and x.month >= 7
        else "old"
    )

    if "Resolution_Time_Hours" in df.columns:
        def categorize_performance(x):
            if pd.isna(x):
                return None
            elif x < 40:
                return "excellent"
            elif x < 80:
                return "good"
            else:
                return "needs_improvement"

    df["performance"] = df["Resolution_Time_Hours"].apply(categorize_performance)

    output_path = "/tmp/cleaned_tickets.csv"
    df.to_csv(output_path, index=False)

    return output_path


def load(file_path: str, engine=None):

    if file_path is None:
        print("No file path provided.")
        return

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        raise ValueError(f"Failed to read file: {e}")

    if df.empty:
        print("No data to load.")
        return



    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns: {missing_cols}")


    if engine is None:
        connection_uri = "postgresql+psycopg2://airflow:airflow@postgres-db:5432/airflow"
        engine = create_engine(connection_uri)

    with engine.begin() as conn:
        # create table if not exists

        if "Submission_Date" in df.columns:
            df["Submission_Date"] = pd.to_datetime(df["Submission_Date"], errors="coerce")

        df.to_sql(
            "customer_support_tickets",
            conn,
            if_exists="append",   # incremental load
            index=False,
            method="multi"
        )

    print(f"Loaded {len(df)} rows into Postgres.")







with DAG(
    dag_id="customer_tickets_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
        op_kwargs={
        "file_path": "{{ ti.xcom_pull(task_ids='extract') }}"
    }
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
        op_kwargs={
        "file_path": "{{ ti.xcom_pull(task_ids='transform') }}"
    }
    )

    extract_task >> transform_task >> load_task 





    



