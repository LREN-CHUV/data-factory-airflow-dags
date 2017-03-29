"""Reorganise folders in order to match the pre-processing pipelines requirements."""

from datetime import datetime, timedelta, time
from textwrap import dedent
from airflow import DAG
from airflow_scan_folder.operators.scan_folder_operator import ScanFlatFolderOperator


def flat_reorganisation_dag(dataset, folder, email_errors_to, trigger_dag_id, depth=1):

    start = datetime.utcnow()
    start = datetime.combine(start.date(), time(start.hour, 0))

    dag_name = '%s_flat_organisation' % dataset.lower().replace(" ", "_")

    # Define the DAG

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': start,
        'retries': 1,
        'retry_delay': timedelta(seconds=120),
        'email': email_errors_to,
        'email_on_failure': True,
        'email_on_retry': True
    }

    dag = DAG(dag_id=dag_name,
              default_args=default_args,
              schedule_interval='@once')

    scan_dirs = ScanFlatFolderOperator(
        task_id='scan_dirs',
        folder=folder,
        trigger_dag_id=trigger_dag_id,
        dataset=dataset,
        depth=depth,
        execution_timeout=timedelta(minutes=30),
        dag=dag)

    scan_dirs.doc_md = dedent("""\
    # Reorganise directories for processing

    Reorganise folder %s (defined by variable __data_folder__).
    """ % folder)

    return dag
