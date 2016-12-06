#
# Functions to start processing a folder or set of folders containing MRI session scans.
#

import logging

import os
import copy

from time import sleep
from airflow import configuration
from datetime import datetime, timedelta

from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException

PRE_PROCESS_DICOM_DAG_NAME = 'pre_process_dicom'

# Folder to scan for new incoming session folders containing DICOM images.
dicom_local_folder = str(configuration.get('mri', 'DICOM_LOCAL_FOLDER'))
dicom_to_nifti_local_folder = str(
    configuration.get('mri', 'NIFTI_LOCAL_FOLDER'))


def trigger_preprocessing(context, dag_run_obj):
    if True:
        session_id = context['params']['session_id']
        logging.info('Trigger preprocessing for : %s', str(session_id))
        # The payload will be available in target dag context as
        # kwargs['dag_run'].conf
        dag_run_obj.payload = context['params']
        dag_run_obj.run_id = session_id + '-' + \
            datetime.today().date().strftime('%Y%m%d-%h%M')
        return dag_run_obj


def is_valid_session_id(session_id):
    sid = session_id.strip().lower()
    return not ('delete' in sid) and not ('phantom' in sid)


def scan_dirs_for_preprocessing(dag):
    def _scan_dirs_for_preprocessing(folder, **kwargs):
        dr = kwargs['dag_run']
        ti = kwargs['task_instance']
        if dr:
            daily_folder_date = dr.execution_date
        else:
            daily_folder_date = ti.execution_date
        look_for_ready_file_marker = daily_folder_date.date() == datetime.today().date()

        if not os.path.exists(folder):
            os.makedirs(folder)

        daily_folder = os.path.join(folder, daily_folder_date.strftime(
            '%Y'), daily_folder_date.strftime('%Y%m%d'))

        if not os.path.isdir(daily_folder):
            daily_folder = os.path.join(
                folder, '2014', daily_folder_date.strftime('%Y%m%d'))

        if not os.path.isdir(daily_folder):
            raise AirflowSkipException

        for fname in os.listdir(daily_folder):
            path = os.path.join(daily_folder, fname)
            if os.path.isdir(path):

                ready_file_marker = os.path.join(path, '.ready')
                if is_valid_session_id(fname) and not look_for_ready_file_marker or os.access(ready_file_marker, os.R_OK):

                    expected_dicom_folder = os.path.join(
                        dicom_local_folder, fname)
                    expected_nifti_folder = os.path.join(
                        dicom_to_nifti_local_folder, fname)

                    if not os.path.isdir(expected_dicom_folder) and not os.path.isdir(expected_nifti_folder):

                        logging.info(
                            'Prepare trigger for preprocessing : %s', str(fname))

                        context = copy.copy(kwargs)
                        context_params = context['params']
                        # Folder containing the DICOM files to process
                        context_params['folder'] = path
                        # Session ID identifies the session for a scan. The
                        # last part of the folder path should match session_id
                        context_params['session_id'] = fname

                        context_params['start_date'] = datetime.utcnow() + timedelta(minutes=1)

                        preprocessing_ingest = TriggerDagRunOperator(
                            # need to wrap task_id in str() because log_name
                            # returns as unicode
                            task_id=str('preprocess_session_%s' % fname),
                            trigger_dag_id=PRE_PROCESS_DICOM_DAG_NAME,
                            python_callable=trigger_preprocessing,
                            params={'folder': path, 'session_id': fname},
                            dag=dag
                        )

                        preprocessing_ingest.execute(context)

                        # Avoid creating Dags at the same time, overwise may
                        # get 'Duplicate entry pre_process_dicom-2016-06-06
                        # 00:01:00 for key dag_id'
                        sleep(1)
    return _scan_dirs_for_preprocessing
