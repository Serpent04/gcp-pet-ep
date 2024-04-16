import airflow
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocStartClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocStopClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator


CLUSTER_NAME ='aw-cluster'
REGION = 'us-central1'
PROJECT_ID = 'gcp-adventure-works'

PUBSUB_TOPIC = 'aw-pubsub'

JDBC_DRIVER = 'gs://jdbc-driver-vk/postgresql-42.6.0.jar'

PYSPARK_URI_1 = 'gs://pyspark-scripts-aw-vk/sql_to_parquet.py'
PYSPARK_URI_2 = 'gs://pyspark-scripts-aw-vk/parquet_to_bigquery.py'


PYSPARK_JOB_1 = {
    'reference': {'project_id': PROJECT_ID},
    'placement': {'cluster_name': CLUSTER_NAME},
    'pyspark_job': {'main_python_file_uri': PYSPARK_URI_1,
                    'jar_file_uris': [JDBC_DRIVER]}
}

PYSPARK_JOB_2 = {
    'reference': {'project_id': PROJECT_ID},
    'placement': {'cluster_name': CLUSTER_NAME},
    'pyspark_job': {'main_python_file_uri': PYSPARK_URI_2}
}

with DAG(
    'aw-dag',
    description='End to end DAG',
    schedule_interval=None,
    start_date=days_ago(1)
) as dag:

    start_dp_cluster = DataprocStartClusterOperator(
        task_id='start_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION
    )

    submit_pyspark_job_1 = DataprocSubmitJobOperator(
        task_id='postgres_to_parquet',
        job=PYSPARK_JOB_1,
        region=REGION,
        project_id=PROJECT_ID
    )

    submit_pyspark_job_2 = DataprocSubmitJobOperator(
        task_id='parquet_to_bigquery',
        job=PYSPARK_JOB_2,
        region=REGION,
        project_id=PROJECT_ID
    )

    stop_dp_cluster = DataprocStopClusterOperator(
        task_id='stop_dataproc_cluster',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION
    )

    notify_pubsub = PubSubPublishMessageOperator(
        task_id='notify_pubsub',
        topic=PUBSUB_TOPIC,
        project_id=PROJECT_ID,
        messages=[{'data': b'DAG completed'}]
    )


    start_dp_cluster >> submit_pyspark_job_1 >> submit_pyspark_job_2 >> stop_dp_cluster >> notify_pubsub