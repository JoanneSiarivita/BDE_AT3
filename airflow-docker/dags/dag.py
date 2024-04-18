import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.gcs_to_postgres import GCSToPostgresOperator
from google.cloud import storage
import psycopg2
import pandas as pd

dag_default_args = {
    'owner': 'Joanne Siarivita',
    'start_date': datetime(2020, 5, 1), 
    'email': [joanne.m.siarivita@student.uts.edu.au],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='assignment3',
    default_args=dag_default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

def read_file_from_gcs(bucket_name, file_name):
    """Read file from Google Cloud Storage."""
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_string()
    return content.decode("utf-8")  # Assuming UTF-8 encoding

def load_data_to_postgres(data, airbnb_listing):
    """Load data into PostgreSQL."""
    conn = psycopg2.connect(
        dbname="postgres", 
        user="dbeaver_gcp",
        password="Gr33nb@nk", 
        host="49.186.99.116",
        port=5432
    )
    cursor = conn.cursor()
    cursor.copy_from(data, airbnb_listing, sep=',')
    conn.commit()
    cursor.close()
    conn.close()

def process_files_from_storage(storage_bucket_assignment3, file_names, table_name):
    """Process files from Google Cloud Storage and load data into PostgreSQL."""
    dfs = []
    for file_name in file_names:
        file_content = read_file_from_gcs(storage_bucket_assignment3, file_name)
        df = pd.read_csv(file_content)
        dfs.append(df)
    
    combined_df = pd.concat(dfs)
    load_data_to_postgres(combined_df.to_csv(index=False), table_name)

process_files_task = PythonOperator(
    task_id='process_files',
    python_callable=process_files_from_storage,
    op_kwargs={'storage_bucket_assignment3': 'storage_bucket_assignment3',
               'file_names': ['01_2021.csv', '02_2021.csv', '03_2021.csv', '04_2021.csv', '05_2020.csv', '06_2020.csv', '07_2020.csv', '08_2020.csv', '09_2020.csv', '10_2020.csv', '11_2020.csv', '12_2020.csv'],  # Add all your file names here
               'table_name': 'airbnb_listing'}, 
    dag=dag
)

# Set task dependencies
process_files_task >> load_data_task