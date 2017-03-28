"""

Poll a base directory for incoming daily EHR database extracts ready for processing.

We assume that CSV files are already anonymised and organised with the following directory structure:

  2016
     _ 20160407
        _ patients.csv
        _ diseases.csv
        _ ...

We are looking for the presence of the .ready marker file indicating that file operations are complete,
or a date folder with a date older than today.

"""

from datetime import datetime, timedelta, time
from textwrap import dedent
from airflow import DAG
from airflow_scan_folder.operators import ScanFlatFolderOperator


def flat_ehr_incoming_dag(dataset, folder, depth, email_errors_to, trigger_dag_id):
    # Folder to scan for new incoming daily EHR-extract folders containing CSV files and other kinds of clinical data.

    # Define the DAG

    start = datetime.utcnow()
    start = datetime.combine(start.date(), time(start.hour, 0))

    dag_name = '%s_mri_flat_etl_incoming' % dataset.lower().replace(" ", "_")

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
        depth=depth,
        trigger_dag_id=trigger_dag_id,
        execution_timeout=timedelta(minutes=30),
        dataset=dataset,
        dag=dag)

    scan_dirs.doc_md = dedent("""\
    # Scan directories for processing

    Scan the folders located inside folder %s (defined by variable __ehr_data_folder__), up to a depth of %s.

    """ % (folder, depth))

    return dag
