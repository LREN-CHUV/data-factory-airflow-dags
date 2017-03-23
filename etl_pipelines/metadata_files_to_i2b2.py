"""

Importing meta-data from meta-data files to I2B2

Configuration variables used:

  * METADATA_FOLDER
  * I2B2_DB

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow import configuration

from airflow_pipeline.operators import PythonPipelineOperator

from i2b2_import import meta_files_import


def metadata_files_to_i2b2_dag(dataset, section, email_errors_to, max_active_runs):

    def arguments_fn():
        metadata_folder = configuration.get(section, 'METADATA_FOLDER')
        i2b2_db = configuration.get(section, 'I2B2_DB')

        return [metadata_folder, i2b2_db]

    def metadata_files_to_i2b2_fn(folder, i2b2_conn, **kwargs):
        meta_files_import.folder2db(folder, i2b2_conn, dataset)

    # Define the DAG

    dag_name = '%s_metadata_files_to' % dataset.lower().replace(" ", "_")

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.now(),
        'retries': 1,
        'retry_delay': timedelta(seconds=120),
        'email': email_errors_to,
        'email_on_failure': True,
        'email_on_retry': True
    }

    dag = DAG(
        dag_id=dag_name,
        default_args=default_args,
        schedule_interval=None,
        max_active_runs=max_active_runs)

    PythonPipelineOperator(
        task_id='metadata_files_to_i2b2_pipeline',
        python_callable=metadata_files_to_i2b2_fn,
        kwargs=arguments_fn,
        pool='io_intensive',
        execution_timeout=timedelta(hours=6),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dag=dag
    )

    return dag
