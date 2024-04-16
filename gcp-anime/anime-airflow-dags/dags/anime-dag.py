import airflow
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitHiveJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator


CLUSTER_NAME = 'anime-pet-vk'
REGION = 'us-central1'
PROJECT_ID = 'gcp-anime-recom'

PUBSUB_TOPIC = 'anime-pubsub'

HIVE_URI = 'gs://hive-scripts-vk/csv_to_parquet.hql'
PYSPARK_URI = 'gs://pyspark-scripts-vk/dataproc-to-bquery.py'


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n2-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 100},
    },
    "software_config": {
        "image_version": "2.2.10-debian12"
    }
}

PYSPARK_JOB = {
    'reference': {'project_id': PROJECT_ID},
    'placement': {'cluster_name': CLUSTER_NAME},
    'pyspark_job': {'main_python_file_uri': PYSPARK_URI}
}

with DAG(
    'anime-dag',
    description='End to end DAG',
    schedule_interval=None,
    start_date=days_ago(1)
) as dag:

    create_dp_cluster = DataprocCreateClusterOperator(
        task_id='create_dp_cluster',
        project_id=PROJECT_ID,
        cluster_config= CLUSTER_CONFIG,
        region=REGION,
        cluster_name= CLUSTER_NAME
    )

    submit_hive_job = DataprocSubmitHiveJobOperator(
        task_id='hive_task',
        query_uri=HIVE_URI,
        cluster_name = CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION
    )

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='pyspark_task',
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID
    )

    delete_dp_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dp_cluster',
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


    create_dp_cluster >> submit_hive_job >> submit_pyspark_job >> delete_dp_cluster >> notify_pubsub