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
from airflow import configuration

# constants

START = datetime.utcnow()
START = datetime.combine(START.date(), time(START.hour, 0))
# START = datetime.combine(datetime.today() - timedelta(days=2), datetime.min.time()) + timedelta(hours=10)
# START = datetime.now()

DAG_NAME = 'daily_pre_process_incoming'

# Folder to scan for new incoming session folders containing DICOM images.
preprocessing_data_folder = str(configuration.get('mri', 'PREPROCESSING_DATA_FOLDER'))
dicom_local_output_folder = str(configuration.get('mri', 'DICOM_LOCAL_OUTPUT_FOLDER'))
dicom_to_nifti_local_output_folder = str(configuration.get('mri', 'NIFTI_LOCAL_OUTPUT_FOLDER'))

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
    dr = kwargs['dag_run']
    daily_folder_date = dr.execution_date
    look_for_ready_file_marker = daily_folder_date.date() == datetime.today().date()

    if not os.path.exists(folder):
        os.makedirs(folder)

    daily_folder = os.path.join(folder, daily_folder_date.strftime('%Y'), daily_folder_date.strftime('%Y%m%d'))

    if not os.path.isdir(daily_folder):
      daily_folder = os.path.join(folder, '2014', daily_folder_date.strftime('%Y%m%d'))
    
    for fname in os.listdir(daily_folder):
        path = os.path.join(daily_folder, fname)
        if os.path.isdir(path):

            ready_file_marker = os.path.join(path, '.ready')
            if not look_for_ready_file_marker or os.access(ready_file_marker, os.R_OK):

                expected_dicom_folder = os.path.join(dicom_local_output_folder, fname)
                expected_nifti_folder = os.path.join(dicom_to_nifti_local_output_folder, fname)

                if not os.path.isdir(expected_dicom_folder) and not os.path.isdir(expected_nifti_folder):

                    logging.info('Prepare trigger for preprocessing : %s', str(fname))
    
                    context = copy.copy(kwargs)
                    context_params = context['params']
                    # Folder containing the DICOM files to process
                    context_params['folder'] = path
                    # Session ID identifies the session for a scan. The last part of the folder path should match session_id
                    context_params['session_id'] = fname
    
                    preprocessing_ingest = TriggerDagRunOperator(
                        # need to wrap task_id in str() because log_name returns as unicode
                        task_id=str('preprocess_ingest_%s' % fname),
                        trigger_dag_id=pre_process_dicom.DAG_NAME,
                        python_callable=trigger_preprocessing,
                        params={'folder': path, 'session_id': fname},
                        dag=dag
                    )
    
                    preprocessing_ingest.execute(context)

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
          schedule_interval='@daily')

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
