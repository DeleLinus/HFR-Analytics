from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import datetime
import os
from google.cloud import storage
import sys

sys.path.append('/opt/airflow/dags/includes/')
from includes.scraper import Scraper

# for auth
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "kagglexfinal-692ca0acf6b4.json"
# define Google Cloud Storage bucket and folder names
BUCKET_NAME = 'kagglexfinal-hfr-bucket'
FOLDER_NAME = 'hfr_raw_data'
DATAFILE_NAME = 'raw_hfr_data.csv'
TIME_STR = datetime.date.today().strftime("%Y-%m-%d")

def scrape_and_store_on_gcs() -> None:
    """
    Scrape data from hfr site and save the csv files in gcs.
    """
    data = Scraper().run()
    print("Saving scraped data...")
    # initialize a Google Cloud Storage client
    storage_client = storage.Client()
    # get the bucket to store the file
    bucket = storage_client.get_bucket(BUCKET_NAME)
    # create a blob (object) in the specified folder
    blob = bucket.blob('{}/{}/{}'.format(FOLDER_NAME,TIME_STR, DATAFILE_NAME))
    # convert the DataFrame to a CSV string and upload it to the blob
    csv_data = data.to_csv(index=False)
    blob.upload_from_string(csv_data, content_type='text/csv')  # https://www.youtube.com/watch?v=IHUJ3g01xmI
    print(f'File {DATAFILE_NAME} uploaded to gs://{BUCKET_NAME}/{FOLDER_NAME}/{TIME_STR}')


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
        dag_id="data_ingestion_gcs_dag",
        schedule_interval="@daily",  # https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html
        default_args=default_args,
        catchup=False,
        max_active_runs=1,
        tags=['upload-gcs']
) as dag:
    start_workflow = EmptyOperator(task_id="start_workflow")
    scrape_and_store_data = PythonOperator(task_id='scrape_and_store_hfr_data', python_callable=scrape_and_store_on_gcs)
    end_workflow = EmptyOperator(task_id="end_workflow")

    # workflow for task direction
    start_workflow >> scrape_and_store_data >> end_workflow
