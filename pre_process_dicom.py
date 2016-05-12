"""

Pre-process DICOM files in a study folder

"""

import logging
import os

from datetime import datetime, timedelta
from functools import partial
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import SpmOperator
from airflow.models import Variable
from airflow import configuration

from util import dicom_import

# constants

DAG_NAME = 'pre_process_dicom'

pipelines_path = str(configuration.get('mri', 'PIPELINES_PATH'))
protocols_file = str(configuration.get('mri', 'PROTOCOLS_FILE'))
dicom_to_nifti_local_output_folder = str(configuration.get('mri', 'NIFTI_LOCAL_OUTPUT_FOLDER'))
dicom_to_nifti_server_output_folder = str(configuration.get('mri', 'NIFTI_SERVER_OUTPUT_FOLDER'))
dicom_to_nifti_pipeline_path = pipelines_path + '/Nifti_Conversion_Pipeline'
neuro_morphometric_atlas_local_output_folder = str(configuration.get('mri', 'NEURO_MORPHOMETRIC_ATLAS_LOCAL_OUTPUT_FOLDER'))
neuro_morphometric_atlas_server_output_folder = str(configuration.get('mri', 'NEURO_MORPHOMETRIC_ATLAS_SERVER_OUTPUT_FOLDER'))
neuro_morphometric_atlas_pipeline_path = pipelines_path + '/NeuroMorphometric_Pipeline/NeuroMorphometric_tbx/label'
mpm_maps_local_output_folder = str(configuration.get('mri', 'MPM_MAPS_LOCAL_OUTPUT_FOLDER'))
mpm_maps_server_output_folder = str(configuration.get('mri', 'MPM_MAPS_SERVER_OUTPUT_FOLDER'))
mpm_maps_pipeline_path = pipelines_path + '/MPMs_Pipeline'

# functions

def extract_dicom_info_fn(**kwargs):
    ti = kwargs['task_instance']
    dr = kwargs['dag_run']
    folder = dr.conf['folder']
    session_id = dr.conf['session_id']

    logging.info('folder %s, session_id %s' % (folder, session_id))

    #dicom_import.dicom2db(folder)

    ti.xcom_push(key='folder', value=folder)
    ti.xcom_push(key='session_id', value=session_id)
    return "ok"

def dicom_to_nifti_pipeline_fn(parent_task, **kwargs):
    engine = kwargs['engine']
    ti = kwargs['task_instance']
    input_data_folder = ti.xcom_pull(key='folder', task_ids=parent_task)
    session_id = ti.xcom_pull(key='session_id', task_ids=parent_task)
    logging.info("DICOM to Nifti pipeline: session_id=%s, input_folder=%s" % (session_id, input_data_folder))
    success = engine.NeuroMorphometric_pipeline(
        input_data_folder,
        session_id,
        dicom_to_nifti_local_output_folder,
        dicom_to_nifti_server_output_folder,
        protocols_file)

    logging.info("SPM returned %s", success)
    if success != 1.0:
        raise RuntimeError('DICOM to Nifti pipeline failed')

    ti.xcom_push(key='folder', value=dicom_to_nifti_local_output_folder)
    ti.xcom_push(key='session_id', value=session_id)
    return success

def neuro_morphometric_atlas_pipeline_fn(parent_task, **kwargs):
    engine = kwargs['engine']
    ti = kwargs['task_instance']
    input_data_folder = ti.xcom_pull(key='folder', task_ids=parent_task)
    session_id = ti.xcom_pull(key='session_id', task_ids=parent_task)
    table_format='csv'
    logging.info("NeuroMorphometric pipeline: session_id=%s, input_folder=%s" % (session_id, input_data_folder))
    success = engine.NeuroMorphometric_pipeline(session_id,
        input_data_folder,
        neuro_morphometric_atlas_local_output_folder,
        neuro_morphometric_atlas_server_output_folder,
        protocols_file,
        table_format)

    logging.info("SPM returned %s", success)
    if success != 1.0:
        raise RuntimeError('NeuroMorphometric pipeline failed')

    ti.xcom_push(key='folder', value=atlasing_output_folder)
    ti.xcom_push(key='session_id', value=session_id)
    return success

def mpm_maps_pipeline(parent_task, **kwargs):
    engine = kwargs['engine']
    ti = kwargs['task_instance']
    input_data_folder = ti.xcom_pull(key='folder', task_ids=parent_task)
    session_id = ti.xcom_pull(key='session_id', task_ids=parent_task)
    pipeline_params_config_file = 'Preproc_mpm_maps_pipeline_config.txt'
    logging.info("MPM Maps pipeline: session_id=%s, input_folder=%s" % (session_id, input_data_folder))
    success = engine.Preproc_mpm_maps(
        input_data_folder,
        session_id,
        local_computation_folder,
        protocols_file,
        pipeline_params_config_file,
        mpms_output_folder)

    logging.info("SPM returned %s", success)
    if success != 1.0:
        raise RuntimeError('MPM Maps pipeline failed')

    ti.xcom_push(key='folder', value=mpms_output_folder)
    ti.xcom_push(key='session_id', value=session_id)
    return success

# Define the DAG

default_args = {
 'owner': 'airflow',
 'pool': 'clinical_vertex',
 'depends_on_past': False,
 'start_date': datetime.now(),
 'retries': 1,
 'retry_delay': timedelta(seconds=120),
 'email_on_failure': True,
 'email_on_retry': True
}
 
dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval=None)

copy_to_shared_folder_cmd = """
    cp -R {{ dag_run.conf["folder"] }} {{ params.dest }}
"""

extract_dicom_info = PythonOperator(
    task_id='extract_dicom_info',
    python_callable=extract_dicom_info_fn,
    provide_context=True,
    execution_timeout=timedelta(hours=1),
    dag=dag)

extract_dicom_info.doc_md = """\
# Extract DICOM information

Read DICOM information from the files stored in the session folder and store that information in the database.
"""

dicom_to_nifti_pipeline = SpmOperator(
    task_id='dicom_to_nifti_pipeline',
    python_callable=partial(dicom_to_nifti_pipeline_fn, 'extract_dicom_info'),
    provide_context=True,
    matlab_paths=[dicom_to_nifti_pipeline_path],
    execution_timeout=timedelta(hours=3),
    dag=dag
    )

dicom_to_nifti_pipeline.set_upstream(extract_dicom_info)

dicom_to_nifti_pipeline.doc_md = """\
# DICOM to Nitfi Pipeline

This function convert the dicom files to Nifti format using the SPM tools and dcm2nii tool developed by Chris Rorden.

Webpage: http://www.mccauslandcenter.sc.edu/mricro/mricron/dcm2nii.html

"""

neuro_morphometric_atlas_pipeline = SpmOperator(
    task_id='neuro_morphometric_atlas_pipeline',
    python_callable=partial(neuro_morphometric_atlas_pipeline_fn, 'dicom_to_nifti_pipeline'),
    provide_context=True,
    matlab_paths=[neuro_morphometric_atlas_pipeline_path],
    execution_timeout=timedelta(hours=3),
    dag=dag
    )

neuro_morphometric_atlas_pipeline.set_upstream(dicom_to_nifti_pipeline)

neuro_morphometric_atlas_pipeline.doc_md = """\
# NeuroMorphometric Pipeline

This function computes an individual Atlas based on the NeuroMorphometrics Atlas. This is based on the NeuroMorphometrics Toolbox.
This delivers three files:

1. Atlas File (*.nii);
2. Volumes of the Morphometric Atlas structures (*.txt);
3. CSV File (.csv) containing the volume, globals, and Multiparametric Maps (R2*, R1, MT, PD) for each structure defined in the Subject Atlas.

"""

mpm_maps_pipeline = SpmOperator(
    task_id='mpm_maps_pipeline',
    python_callable=partial(mpm_maps_pipeline_fn, 'neuro_morphometric_atlas_pipeline'),
    provide_context=True,
    matlab_paths=[mpm_maps_pipeline_path],
    execution_timeout=timedelta(hours=3),
    dag=dag
    )

mpm_maps_pipeline.set_upstream(neuro_morphometric_atlas_pipeline)

mpm_maps_pipeline.doc_md = """\
# MPM Maps Pipeline

This function computes the Multiparametric Maps (MPMs) (R2*, R1, MT, PD) and brain segmentation in different tissue maps.
All computation was programmed based on the LREN database structure. The MPMs are calculated locally in 'OutputFolder' and finally copied to 'ServerFolder'.

"""
