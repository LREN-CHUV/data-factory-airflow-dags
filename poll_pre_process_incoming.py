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
import os
import copy

from datetime import datetime, timedelta, time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable

# functions

def trigger_preprocessing(context, dag_run_obj):
    if True:
        session_id = context['params']['session_id']
        logging.info('Trigger preprocessing for : %s', str(session_id))
        # The payload will be available in target dag context as kwargs['dag_run'].conf
        dag_run_obj.payload = context['params']
        dag_run_obj.run_id = session_id
        return dag_run_obj

def scan_dirs_for_preprocessing(folder, **kwargs):
    if not os.path.exists(folder):
        os.makedirs(folder)

    for fname in os.listdir(folder):
        path = os.path.join(folder, fname)
        if os.path.isdir(path):
            ready_file_marker = os.path.join(path, '.ready')
            proccessing_file_marker = os.path.join(path, '.processing')
            if os.access(ready_file_marker, os.R_OK) and not os.access(proccessing_file_marker, os.R_OK):
                logging.info('Prepare trigger for preprocessing : %s', str(fname))

                context = copy.copy(kwargs)
                context_params = context['params']
                context_params['folder'] = folder
                context_params['session_id'] = fname

                preprocessing_ingest = TriggerDagRunOperator(
                    # need to wrap task_id in str() because log_name returns as unicode
                    task_id=str('preprocess_ingest_%s' % fname),
                    trigger_dag_id=pre_process_dicom.DAG_NAME,
                    python_callable=trigger_preprocessing,
                    params={'folder': folder, 'session_id': fname},
                    dag=dag
                )

                preprocessing_ingest.execute(context)

                # Create .processing marker file in the folder marked for processing to avoid duplicate processing
                open(proccessing_file_marker, 'a').close()

# constants

START = datetime.utcnow()
START = datetime.combine(START.date(), time(START.hour, 0))
# START = datetime.combine(datetime.today() - timedelta(days=2), datetime.min.time()) + timedelta(hours=10)
# START = datetime.now()

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
          schedule_interval='* * * * *')

try:
    preprocessing_data_folder = Variable.get("preprocessing_data_folder")
except:
    preprocessing_data_folder = "/tmp/data/incoming"

scan_ready_dirs = PythonOperator(
    task_id='scan_dirs_ready_for_preprocessing',
    python_callable=scan_dirs_for_preprocessing,
    op_args=[preprocessing_data_folder],
    provide_context=True,
    dag=dag)

scan_ready_dirs.doc_md = """\
# Scan directories ready for processing

Scan the session folders starting from the root folder %s (defined by variable __preprocessing_data_folder__).

It looks for the presence of a .ready marker file to mark that session folder as ready for processing, but it
will skip it if contains the marker file .processing indicating that processing has already started.
""" % preprocessing_data_folder
