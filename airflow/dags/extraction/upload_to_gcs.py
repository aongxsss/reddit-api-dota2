import configparser
import pathlib
import sys
from google.cloud import storage
from val_date import validate_input

parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
parser.read(f"{script_path}/config.conf")
BUCKET_NAME = parser.get("gcp_config", "bucket_name")

try:
    output_name = sys.argv[1]
except Exception as e:
    print(f"Command line argument not passed. Error {e}")
    sys.exit(1)

FILENAME = f"{output_name}.csv"
def main():
    """Upload input file to GCS bucket"""
    validate_input(output_name)
    client = connect_to_gcs()
    upload_file_to_gcs(client) 
   
def connect_to_gcs():
    """Connect to GCS Instance"""
    try:
        client = storage.Client() 
        return client
    except Exception as e:
        print(f"Can't connect to GCS. Error: {e}")
        sys.exit(1)

def upload_file_to_gcs(client):
    """Upload file to GCS Bucket"""
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILENAME)  
    blob.upload_from_filename(f"/tmp/{FILENAME}")
    
if __name__ == "__main__":
    main()