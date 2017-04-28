"""Import imaging metadata from various files to I2B2"""

from datetime import datetime, timedelta

from airflow import DAG

from common_steps import initial_step
from common_steps.prepare_pipeline import prepare_pipeline
from metadata_steps.metadata_to_i2b2 import metadata_to_i2b2_pipeline_cfg


def import_metadata_dag(dataset, section, email_errors_to, max_active_runs, metadata_pipelines=''):

    # Define the DAG

    dag_name = '%s_import_metadata' % dataset.lower().replace(" ", "_")

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

    upstream_step = prepare_pipeline(dag, initial_step, True)

    if 'metadata_to_i2b2' in metadata_pipelines:
        metadata_to_i2b2_pipeline_cfg(dag, upstream_step, section)
    # endif

    return dag
