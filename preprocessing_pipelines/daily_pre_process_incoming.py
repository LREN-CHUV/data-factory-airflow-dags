"""

Poll a base directory for incoming Dicom files ready for processing. We assume that
Dicom files are already processed by the hierarchize.sh script with the following directory structure:

  2016
     _ 20160407
        _ PR01471_CC082251
           _ .ready
           _ 1
              _ al_B1mapping_v2d
              _ gre_field_mapping_1acq_rl
              _ localizer

We are looking for the presence of the .ready marker file indicating that pre-processing of an MRI session is complete,
or a date folder with a date older than today.

"""

from datetime import datetime, timedelta, time
from textwrap import dedent
from airflow import DAG
from airflow_scan_folder.operators import ScanDailyFolderOperator


def daily_preprocess_incoming_dag(dataset, folder, email_errors_to, trigger_dag_id):
    # Folder to scan for new incoming session folders containing DICOM images.

    # constants

    START = datetime.utcnow()
    START = datetime.combine(START.date(), time(START.hour, 0))

    DAG_NAME = '%s_mri_daily_pre_process_incoming' % dataset.lower().replace(" ", "_")

    # Define the DAG

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': START,
        'retries': 1,
        'retry_delay': timedelta(seconds=120),
        'email': email_errors_to,
        'email_on_failure': True,
        'email_on_retry': True
    }

    dag = DAG(dag_id=DAG_NAME,
              default_args=default_args,
              schedule_interval='@daily')

    scan_dirs = ScanDailyFolderOperator(
        task_id='scan_dirs',
        folder=folder,
        trigger_dag_id=trigger_dag_id,
        execution_timeout=timedelta(minutes=30),
        dataset=dataset,
        dag=dag)

    scan_dirs.doc_md = dedent("""\
    # Scan directories for processing

    Scan the session folders located inside folder %s (defined by variable __preprocessing_data_folder__) and organised
    by daily folders.

    Daily folders older than today are always processed, and today's folder content is skipped unless a .ready marker
    file is found.
    """ % folder)

    return dag
