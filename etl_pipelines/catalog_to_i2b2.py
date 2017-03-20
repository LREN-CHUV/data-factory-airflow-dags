"""

Importing meta-data from data-catalog to I2B2

Configuration variables used:

  * DATA_CATALOG_DB
  * I2B2_DB

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow import configuration

from airflow_pipeline.operators import PythonPipelineOperator

from i2b2_import import data_catalog_import


def catalog_to_i2b2_dag(dataset, section, email_errors_to, max_active_runs):

    def arguments_fn():
        data_catalog_db = configuration.get(section, 'DATA_CATALOG_DB')
        i2b2_db = configuration.get(section, 'I2B2_DB')

        return [data_catalog_db, i2b2_db]

    def catalog_to_i2b2_fn(data_catalog_conn, i2b2_conn, **kwargs):
        data_catalog_import.catalog2i2b2(data_catalog_conn, i2b2_conn)

    # Define the DAG

    dag_name = '%s_catalog_to' % dataset.lower().replace(" ", "_")

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
        task_id='catalog_to_i2b2_pipeline',
        python_callable=catalog_to_i2b2_fn,
        kwargs=arguments_fn,
        pool='io_intensive',
        execution_timeout=timedelta(hours=6),
        on_skip_trigger_dag_id='mri_notify_skipped_processing',
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dag=dag
    )

    return dag
