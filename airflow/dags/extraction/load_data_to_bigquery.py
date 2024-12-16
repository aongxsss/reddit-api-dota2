import configparser
import pathlib
import sys
from google.cloud import bigquery
from val_date import validate_input
from airflow.utils.log.logging_mixin import LoggingMixin

parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
parser.read(f"{script_path}/config.conf")

PROJECT_ID = parser.get("gcp_config", "project_id")
DATASET_ID = parser.get("gcp_config", "dataset_id")
TABLE_NAME = parser.get("gcp_config", "table_name")
BUCKET_NAME = parser.get("gcp_config", "bucket_name")

try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Command line argument not passed. Error {e}")
    sys.exit(1)


file_path = f"gs://{BUCKET_NAME}/{output_name}.csv"

schema = [
    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("score", "INTEGER"),
    bigquery.SchemaField("num_comments", "INTEGER"),
    bigquery.SchemaField("author", "STRING"),
    bigquery.SchemaField("created_utc", "TIMESTAMP"),
    bigquery.SchemaField("url", "STRING"),
    bigquery.SchemaField("upvote_ratio", "FLOAT"),
    bigquery.SchemaField("over_18", "BOOLEAN"),
    bigquery.SchemaField("edited", "BOOLEAN"),
    bigquery.SchemaField("spoiler", "BOOLEAN"),
    bigquery.SchemaField("stickied", "BOOLEAN"),
]

def main():
    """Upload file from GCS to BigQuery Table"""
    validate_input(output_name)
    client = connect_to_bigquery()
    load_data_into_bigquery(client)


def connect_to_bigquery():
    """Connect to BigQuery instance"""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        return client
    except Exception as e:
        print(f"Unable to connect to BigQuery. Error {e}")
        sys.exit(1)


def load_data_into_bigquery(client):
    """Load data from GCS into BigQuery"""
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1, 
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace data in table
        # write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_uri(file_path, table_id, job_config=job_config)
    LoggingMixin().log.info(f"Starting job {load_job.job_id}")
    load_job.result()  

    # Check number of rows loaded
    destination_table = client.get_table(table_id)
    LoggingMixin().log.info(f"Loaded {destination_table.num_rows} rows into {table_id}.")


if __name__ == "__main__":
    main()
