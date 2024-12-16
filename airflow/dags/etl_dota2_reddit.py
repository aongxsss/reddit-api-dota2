from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime

output_name = datetime.now().strftime("%Y%m%d")

schedule_interval = "@daily"
start_date = days_ago(1)

default_args = {
    "owner": "aongxsss",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="reddit_data_pipeline",
    description="ETL reddit data",
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=["RedditETL"],
) as dag:
    
    # Task to extract Reddit data
    extract_reddit_data = BashOperator(
        task_id="extract_reddit_data",
        bash_command=f"python /opt/airflow/dags/extraction/extract_data_reddit.py {output_name}",
    )
    extract_reddit_data.doc_md = "Extract Reddit data and store as CSV"
    
    # Task to upload CSV data to Google Cloud Storage
    upload_to_gcs = BashOperator(
        task_id="upload_to_gcs",
        bash_command=f"python /opt/airflow/dags/extraction/upload_to_gcs.py {output_name}",
    )
    upload_to_gcs.doc_md = "Upload Reddit CSV data to Google Cloud Storage"
    
    # Task to load data from GCS to BigQuery
    load_data_to_bigquery = BashOperator(
        task_id="load_data_to_bigquery",
        bash_command=f"python /opt/airflow/dags/extraction/load_data_to_bigquery.py {output_name}",
    )
    load_data_to_bigquery.doc_md = "Load Reddit CSV data from GCS to BigQuery"
    
    
    extract_reddit_data >> upload_to_gcs >> load_data_to_bigquery
