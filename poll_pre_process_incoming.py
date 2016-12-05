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

We are looking for the presence of the .ready marker file indicating that pre-processing of an MRI session is complete.

"""

from datetime import datetime, timedelta, time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow import configuration

from util.mri_session_folder import scan_dirs_for_preprocessing

# constants

START = datetime.utcnow()
START = datetime.combine(START.date(), time(START.hour, 0))
# START = datetime.combine(datetime.today() - timedelta(days=2), datetime.min.time()) + timedelta(hours=10)
# START = datetime.now()

DAG_NAME = 'poll_pre_process_incoming'

# Folder to scan for new incoming session folders containing DICOM images.
preprocessing_data_folder = str(
    configuration.get('mri', 'PREPROCESSING_DATA_FOLDER'))

# Define the DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START,
    'retries': 1,
    'retry_delay': timedelta(seconds=120),
    'email': 'ludovic.claude@chuv.ch',
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(dag_id=DAG_NAME,
          default_args=default_args,
          schedule_interval='*/10 * * * *')

scan_ready_dirs = PythonOperator(
    task_id='scan_dirs_ready_for_preprocessing',
    python_callable=scan_dirs_for_preprocessing(dag),
    op_args=[preprocessing_data_folder],
    provide_context=True,
    dag=dag)

scan_ready_dirs.doc_md = """\
# Scan directories ready for processing

Scan the session folders starting from the root folder %s (defined by variable __preprocessing_data_folder__).

It looks for the presence of a .ready marker file to mark that session folder as ready for processing, but it
will skip it if contains the marker file .processing indicating that processing has already started.
""" % preprocessing_data_folder
