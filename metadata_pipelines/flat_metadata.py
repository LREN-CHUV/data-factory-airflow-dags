"""Import metadata from various files into I2B2 DB"""

from datetime import datetime, timedelta, time
from textwrap import dedent
from airflow import DAG
from airflow_scan_folder.operators.scan_folder_operator import ScanFlatFolderOperator
from airflow_scan_folder.operators.common import default_extract_context
from airflow_scan_folder.operators.common import default_accept_folder
from airflow_scan_folder.operators.common import default_trigger_dagrun


def flat_metadata_dag(dataset, folder, email_errors_to, trigger_dag_id):

    start = datetime.utcnow()
    start = datetime.combine(start.date(), time(start.hour, 0))

    dag_name = '%s_flat_metadata' % dataset.lower().replace(" ", "_")

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
        dataset=dataset,
        folder=folder,
        trigger_dag_id=trigger_dag_id,
        trigger_dag_run_callable=default_trigger_dagrun,
        extract_context_callable=default_extract_context,
        accept_folder_callable=default_accept_folder,
        execution_timeout=timedelta(minutes=30),
        dag=dag)

    scan_dirs.doc_md = dedent("""\
    # Import metadata from various files into I2B2 DB

    Input folder %s (defined by variable __data_folder__).
    """ % folder)

    return dag
