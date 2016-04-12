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

import logging
import pre_process_dicom
import pprint
import os, sys

from datetime import datetime, timedelta, time
from airflow import DAG
from airflow.operators import BashOperator, PythonOperator, TriggerDagRunOperator
from airflow.models import Variable

# functions

def trigger_preprocessing(context, dag_run_obj):
    if True:
        session_id = context['params']['session_id']
        logging.info('Trigger preprocessing for : %s', str(session_id))
        dag_run_obj.payload = context['params']
        dag_run_obj.run_id = str(session_id + '__%s' % datetime.now().strftime("%Y-%m-%dT%H:%M:%s"))
        return dag_run_obj

# constants

START = datetime.utcnow()
START = datetime.combine(START.date(), time(START.hour, 0))
#START = datetime.combine(datetime.today() - timedelta(days=2), datetime.min.time()) + timedelta(hours=10)
#START = datetime.now()

DAG_NAME = 'poll_pre_process_incoming'

# Define the DAG

default_args = {
 'owner': 'airflow',
 'depends_on_past': False,
 'start_date': START,
 'retries': 1,
 'retry_delay': timedelta(seconds=120),
 'email_on_failure': True,
 'email_on_retry': True
}

dag = DAG(dag_id=DAG_NAME,
          default_args=default_args,
          schedule_interval='0 * * * *')

try:
    preprocessing_data_folder = Variable.get("preprocessing_data_folder")
except:
    preprocessing_data_folder = "/tmp/data/incoming"

scan_ready_dirs = BashOperator(
    task_id='scan_dirs_ready_for_preprocessing',
    bash_command="echo 'Scaning directories ready for processing'",
    dag=dag)

scan_ready_dirs.doc_md = """\
# Scan directories ready for processing

Scan the session folders starting from the root folder defined by variable __preprocessing_data_folder__.

It looks for the presence of a .ready marker file to mark that session folder as ready for processing, but it
will skip it if contains the marker file .processing indicating that processing has already started.
"""

poll_complete = BashOperator(
    task_id='poll_complete',
    bash_command="echo 'Poll complete'",
    dag=dag)

scan_ready_dirs.doc_md = """\
# Scan directories ready for processing

Scan the session folders starting from the root folder defined by variable __preprocessing_data_folder__.

It looks for the presence of a .ready marker file to mark that session folder as ready for processing, but it
will skip it if contains the marker file .processing indicating that processing has already started.
"""

if not os.path.exists(preprocessing_data_folder):
    os.makedirs(preprocessing_data_folder)

for fname in os.listdir(preprocessing_data_folder):
    path = os.path.join(preprocessing_data_folder, fname)
    if os.path.isdir(path):
        ready_file_marker = os.path.join(path, '.ready')
        proccessing_file_marker = os.path.join(path, '.processing')
        if os.access(ready_file_marker, os.R_OK) and not os.access(proccessing_file_marker, os.R_OK):
            logging.info('Prepare trigger for preprocessing : %s', str(fname))

            preprocessing_ingest = TriggerDagRunOperator(
                # need to wrap task_id in str() because log_name returns as unicode
                task_id=str('preprocess_ingest_%s' % fname),
                trigger_dag_id=pre_process_dicom.DAG_NAME,
                python_callable=trigger_preprocessing,
                params={'folder': path, 'session_id': fname},
                dag=dag
            )

            preprocessing_ingest.set_upstream(scan_ready_dirs)
            preprocessing_ingest.set_downstream(poll_complete)
