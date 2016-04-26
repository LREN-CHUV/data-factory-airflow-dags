"""

Pre-process DICOM files in a study folder

"""

import logging
import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators import SpmOperator
from airflow.models import Variable

from util import dicom_import

# constants

DAG_NAME = 'pre_process_dicom'

try:
    shared_data_folder = Variable.get("shared_data_folder")
except:
    shared_data_folder = "/data/shared"

try:
    local_computation_folder = Variable.get("local_computation_folder")
except:
    local_computation_folder = "/tmp/pipelines"

try:
    atlasing_output_folder = Variable.get("atlasing_output_folder")
except:
    atlasing_output_folder = "/data/results"

try:
    mpms_output_folder = Variable.get("mpms_output_folder")
except:
    mpms_output_folder = "/data/results"

try:
    pipelines_path = Variable.get("pipelines_path")
except:
    pipelines_path = "/home/ludovic/Projects/LREN/automated-pipeline/Pipelines"

try:
    protocols_file = Variable.get("protocols_file")
except:
    protocols_file = "/home/ludovic/Projects/LREN/automated-pipeline/Protocols_definition.txt"

logging.info("protocols_file: %s" % protocols_file)

try:
    neuro_morphometric_pipeline_path = Variable.get('neuro_morphometric_pipeline_path')
except:
    neuro_morphometric_pipeline_path = pipelines_path + '/NeuroMorphometric_Pipeline/NeuroMorphometric_tbx/label'

try:
    mpm_maps_pipeline_path = Variable.get('mpm_maps_pipeline_path')
except:
    mpm_maps_pipeline_path = pipelines_path + '/MPMs_Pipeline'

logging.info("mpm_maps_pipeline_path: %s" % mpm_maps_pipeline_path)

# functions

def extractDicomInfo(**kwargs):
    ti = kwargs['task_instance']
    #lastRun = ti.xcom_pull(task_ids='format_last_run_date')

    dr = kwargs['dag_run']
    folder = dr.conf['folder']
    session_id = dr.conf['session_id']

    logging.info('folder %s, session_id %s' % (folder, session_id))

    #dicom_import.dicom2db(folder)

    ti.xcom_push(key='folder', value=folder)
    ti.xcom_push(key='session_id', value=session_id)
    return "ok"

def neuroMorphometricPipeline(**kwargs):
    engine = kwargs['engine']
    ti = kwargs['task_instance']
    input_data_folder = ti.xcom_pull(key='folder', task_ids='extract_dicom_info')
    session_id = ti.xcom_pull(key='session_id', task_ids='extract_dicom_info')
    table_format='csv'
    logging.info("NeuroMorphometric pipeline: session_id=%s, input_folder=%s" % (session_id, input_data_folder))
    success = engine.NeuroMorphometric_pipeline(session_id,
        input_data_folder,
        local_computation_folder,
        atlasing_output_folder,
        protocols_file,
        table_format)

    logging.info("SPM returned %s", success)
    if success != 1.0:
        raise RuntimeError('NeuroMorphometric pipeline failed')

    ti.xcom_push(key='folder', value=atlasing_output_folder)
    ti.xcom_push(key='session_id', value=session_id)
    return success

def mpmMapsPipeline(**kwargs):
    engine = kwargs['engine']
    ti = kwargs['task_instance']
    input_data_folder = ti.xcom_pull(key='folder', task_ids='neuro_morphometric_pipeline')
    session_id = ti.xcom_pull(key='session_id', task_ids='neuro_morphometric_pipeline')
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
    python_callable=extractDicomInfo,
    provide_context=True,
    execution_timeout=timedelta(hours=1),
    dag=dag)

extract_dicom_info.doc_md = """\
# Extract DICOM information

Read DICOM information from the files stored in the session folder and store that information in the database.
"""

#copy_to_shared_folder = BashOperator(
#    task_id='copy_to_shared_folder',
#    bash_command=copy_to_shared_folder_cmd,
#    execution_timeout=timedelta(hours=3),
#    pool='data_transfers',
#    provide_context=True,
#    params={'dest':shared_data_folder},
#    dag=dag)
#copy_to_shared_folder.set_upstream(extract_dicom_info)
#
#copy_to_shared_folder.doc_md = """\
## Copy data to shared folder
#
#"""


neuro_morphometric_pipeline = SpmOperator(
    task_id='neuro_morphometric_pipeline',
    python_callable=neuroMorphometricPipeline,
    provide_context=True,
    matlab_paths=[neuro_morphometric_pipeline_path],
    execution_timeout=timedelta(hours=3),
    dag=dag
    )

neuro_morphometric_pipeline.set_upstream(extract_dicom_info)

neuro_morphometric_pipeline.doc_md = """\
# NeuroMorphometric Pipeline

This function computes an individual Atlas based on the NeuroMorphometrics Atlas. This is based on the NeuroMorphometrics Toolbox.
This delivers three files:

1. Atlas File (*.nii);
2. Volumes of the Morphometric Atlas structures (*.txt);
3. CSV File (.csv) containing the volume, globals, and Multiparametric Maps (R2*, R1, MT, PD) for each structure defined in the Subject Atlas.

"""

mpm_maps_pipeline = SpmOperator(
    task_id='MPM_Maps_pipeline',
    python_callable=mpmMapsPipeline,
    provide_context=True,
    matlab_paths=[mpm_maps_pipeline_path],
    execution_timeout=timedelta(hours=3),
    dag=dag
    )

mpm_maps_pipeline.set_upstream(neuro_morphometric_pipeline)

mpm_maps_pipeline.doc_md = """\
# MPM Maps Pipeline

This function computes the Multiparametric Maps (MPMs) (R2*, R1, MT, PD) and brain segmentation in different tissue maps.
All computation was programmed based on the LREN database structure. The MPMs are calculated locally in 'OutputFolder' and finally copied to 'ServerFolder'.

"""
