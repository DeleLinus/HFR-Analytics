from airflow import models
from datetime import timedelta
import datetime
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor 

# Calculate the date for the previous day
yesterday = datetime.date.today() - datetime.timedelta(days=1)
date_str = yesterday.strftime("%Y-%m-%d")

# dataproc constants
CLUSTER_NAME = 'dataproc-airflow-cluster-hfr'
REGION='us-central1' # region
PROJECT_ID='kagglexfinal' #project name
PYSPARK_URI='gs://kagglexfinal-hfr-bucket/code/main_spark_transformation.py' # spark job location in cloud storage

# GCS constants
GCP_CONN_ID = 'google_cloud_default' # just have to be this value when creating the airflow connection, else, the dataproc raises error.
GCS_BUCKET_NAME = 'kagglexfinal-hfr-bucket'
FOLDER_NAME_RAW = f'hfr_raw_data/{date_str}'
FOLDER_NAME_TRANSFORMED = "hfr_processed_data"
GCS_KEY_NAME = 'raw_hfr_data.csv'


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 500},
    }
}


PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}


with models.DAG(
    "dataproc_airflow_gcp_to_gbq",
    schedule_interval='@daily',  
    start_date=days_ago(1),
    catchup=False,
    tags=["dataproc_airflow"],
) as dag:
    
    start_workflow = EmptyOperator(task_id="start_workflow")

    verify_key_existence = GCSObjectExistenceSensor(
                                task_id="verify_key_existence",
                                google_cloud_conn_id=GCP_CONN_ID,
                                bucket=GCS_BUCKET_NAME,
                                object=f"{FOLDER_NAME_RAW}/{GCS_KEY_NAME}"
    )
    
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_job = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

     
    # define parameters for each Parquet file to BigQuery table load with schema inference
    table_1_task = GCSToBigQueryOperator(
        task_id='load_factPersonnel_table',
        bucket=GCS_BUCKET_NAME,
        source_objects= [f'{FOLDER_NAME_TRANSFORMED}/factPersonnel.parquet/*.snappy.parquet'],
        destination_project_dataset_table='kagglexfinal.hfr_dataset.factPersonnel', 
        create_disposition='CREATE_IF_NEEDED', # create the table if it doesn't exist
        write_disposition='WRITE_TRUNCATE', # replace data if the table already exists
        source_format= 'PARQUET',
        autodetect =True,  # allow GBQ to infer the schema
    )

    table_2_task = GCSToBigQueryOperator(
        task_id='load_dimInstitutions_table',
        bucket=GCS_BUCKET_NAME,
        source_objects= [f'{FOLDER_NAME_TRANSFORMED}/dimInstitutions.parquet/*.snappy.parquet'],
        destination_project_dataset_table='kagglexfinal.hfr_dataset.dimInstitutions', 
        create_disposition='CREATE_IF_NEEDED', # create the table if it doesn't exist
        write_disposition='WRITE_TRUNCATE', # replace data if the table already exists
        source_format= 'PARQUET',
        autodetect =True,  # allow GBQ to infer the schema
    )

    table_3_task = GCSToBigQueryOperator(
        task_id='load_dimLocation_table',
        bucket=GCS_BUCKET_NAME,
        source_objects= [f'{FOLDER_NAME_TRANSFORMED}/dimLocation.parquet/*.snappy.parquet'],
        destination_project_dataset_table='kagglexfinal.hfr_dataset.dimLocation', 
        create_disposition='CREATE_IF_NEEDED', # create the table if it doesn't exist
        write_disposition='WRITE_TRUNCATE', # replace data if the table already exists
        source_format= 'PARQUET',
        autodetect =True,  # allow GBQ to infer the schema
    )

    table_4_task = GCSToBigQueryOperator(
        task_id='load_dimContacts_table',
        bucket=GCS_BUCKET_NAME,
        source_objects= [f'{FOLDER_NAME_TRANSFORMED}/dimContacts.parquet/*.snappy.parquet'],
        destination_project_dataset_table='kagglexfinal.hfr_dataset.dimContacts', 
        create_disposition='CREATE_IF_NEEDED', # create the table if it doesn't exist
        write_disposition='WRITE_TRUNCATE', # replace data if the table already exists
        source_format= 'PARQUET',
        autodetect =True,  # allow GBQ to infer the schema
    )

    table_5_task = GCSToBigQueryOperator(
        task_id='load_dimCommonServices_table',
        bucket=GCS_BUCKET_NAME,
        source_objects= [f'{FOLDER_NAME_TRANSFORMED}/dimCommonServices.parquet/*.snappy.parquet'],
        destination_project_dataset_table='kagglexfinal.hfr_dataset.dimCommonServices', 
        create_disposition='CREATE_IF_NEEDED', # create the table if it doesn't exist
        write_disposition='WRITE_TRUNCATE', # replace data if the table already exists
        source_format= 'PARQUET',
        autodetect =True,  # allow GBQ to infer the schema
    )

    table_6_task = GCSToBigQueryOperator(
        task_id='load_dimOperationalDay_table',
        bucket=GCS_BUCKET_NAME,
        source_objects= [f'{FOLDER_NAME_TRANSFORMED}/dimOperationalDay.parquet/*.snappy.parquet'],
        destination_project_dataset_table='kagglexfinal.hfr_dataset.dimOperationalDay', 
        create_disposition='CREATE_IF_NEEDED', # create the table if it doesn't exist
        write_disposition='WRITE_TRUNCATE', # replace data if the table already exists
        source_format= 'PARQUET',
        autodetect =True,  # allow GBQ to infer the schema
    )

    table_7_task = GCSToBigQueryOperator(
        task_id='load_dimSpecializedServices_table',
        bucket=GCS_BUCKET_NAME,
        source_objects= [f'{FOLDER_NAME_TRANSFORMED}/dimSpecializedServices.parquet/*.snappy.parquet'],
        destination_project_dataset_table='kagglexfinal.hfr_dataset.dimSpecializedServices', 
        create_disposition='CREATE_IF_NEEDED', # create the table if it doesn't exist
        write_disposition='WRITE_TRUNCATE', # replace data if the table already exists
        source_format= 'PARQUET',
        autodetect =True,  # allow GBQ to infer the schema
    )

       
    

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION
    )

    end_workflow = EmptyOperator(task_id="end_workflow")

    start_workflow >> verify_key_existence >> create_cluster >> submit_job >> \
    table_1_task >> table_2_task >> table_3_task >> table_4_task >> \
        table_5_task >> table_6_task >> table_7_task >> [delete_cluster] >> end_workflow 