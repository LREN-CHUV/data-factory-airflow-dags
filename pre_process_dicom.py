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
PROTOCOLS_FILE='Protocols_definition.txt'

try:
    shared_data_folder = Variable.get("shared_data_folder")
except:
    shared_data_folder = "/tmp/data/shared"

try:
    local_computation_folder = Variable.get("local_computation_folder")
except:
    local_computation_folder = "/home/ludovic/tmp/test"

try:
    atlasing_output_folder = Variable.get("atlasing_output_folder")
except:
    atlasing_output_folder = "/home/ludovic/tmp/results"

try:
    pipelines_path = Variable.get("pipelines_path")
except:
    pipelines_path = "/home/ludovic/tmp/For_Ludovic/Automatic_Computation_under_Organization/Pipelines"

try:
    neuro_morphometric_pipeline_path = Variable.get('neuro_morphometric_pipeline_path')
except:
    neuro_morphometric_pipeline_path = pipelines_path + '/NeuroMorphometric_Pipeline/NeuroMorphometric_tbx/label'

# functions

def extractDicomInfo(**kwargs):
    ti = kwargs['task_instance']
    #lastRun = ti.xcom_pull(task_ids='format_last_run_date')

    dr = kwargs['dag_run']
    folder = dr.conf['folder']
    session_id = dr.conf['session_id']

    logging.info('folder %s, session_id %s' % (folder, session_id))

    # ti.xcom_push({'patient':patient})

    dicom_import.dicom2db(folder)

    return ""

def neuroMorphometricPipeline(**kwargs):
    engine = kwargs['engine']
    #ti = kwargs['task_instance']
    dr = kwargs['dag_run']
    input_data_folder = dr.conf['folder']
    input_data_folder = os.sep.join(input_data_folder.split(os.sep)[:-1])
    subj_id = dr.conf['session_id']
    table_format='csv'
    success = engine.NeuroMorphometric_pipeline(subj_id,
        input_data_folder,
        local_computation_folder,
        atlasing_output_folder,
        PROTOCOLS_FILE,
        table_format)

    logging.info("SPM returned %s", success)
    if success == 0:
        raise RuntimeError('NeuroMorphometric pipeline failed')
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

mark_start_of_processing_cmd = """
    touch {{ dag_run.conf["folder"] }}/.processing
"""

copy_to_shared_folder_cmd = """
    cp -R {{ dag_run.conf["folder"] }} {{ params.dest }}
"""

mark_start_of_processing = BashOperator(
    task_id='mark_start_of_preprocessing',
    bash_command=mark_start_of_processing_cmd,
    provide_context=True,
    dag=dag)

mark_start_of_processing.doc_md = """\
# Mark start of processing on a session folder

Create a marker file .processing inside the session folder to indicate that processing has started on that folder.
"""

extract_dicom_info = PythonOperator(
    task_id='extract_dicom_info',
    python_callable=extractDicomInfo,
    execution_timeout=timedelta(hours=3),
    provide_context=True,
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
    dag=dag
    )

neuro_morphometric_pipeline.set_upstream(extract_dicom_info)

neuro_morphometric_pipeline.doc_md = """\
# NeuroMorphometric Pipeline

This function computes an individual Atlas based on the NeuroMorphometrics Atlas. This is based on the NeuroMorphometrics Toolbox.
This delivers three files:

1. Atlas File (*.nii);
2. Volumes of the Morphometric Atlas structures (*.txt);
3. Excel File (.xls) containing the volume, globals, and Multiparametric Maps (R2*, R1, MT, PD) for each structure defined in the Subject Atlas.

"""
