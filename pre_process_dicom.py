"""

Pre-process DICOM files in a study folder

"""

import logging
  
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
    shared_data_folder = "/tmp/data/shared"

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

def spmTest(**kwargs):
    engine = kwargs['engine']
    SubjID='PR01693_NO310167'
    InputDataFolder='/home/ludovic/tmp/For_Ludovic/Automatic_Computation_under_Organization/Sample_Data/For_Neuromorphics_Pipeline'
    LocalFolder='/home/ludovic/tmp/test'
    AtlasingOutputFolder='/home/ludovic/tmp/results'
    ProtocolsFile='Protocols_definition.txt'
    TableFormat='csv'
    success = engine.NeuroMorphometric_pipeline(SubjID,InputDataFolder,LocalFolder,AtlasingOutputFolder,ProtocolsFile,TableFormat)
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
    matlab_paths=['/home/ludovic/tmp/For_Ludovic/Automatic_Computation_under_Organization/Pipelines/NeuroMorphometric_Pipeline/NeuroMorphometric_tbx/label'],
    dag=dag)
extract_dicom_info.set_upstream(mark_start_of_processing)

extract_dicom_info.doc_md = """\
# Extract DICOM information

Read DICOM information from the files stored in the session folder and store that information in the database.
"""

copy_to_shared_folder = BashOperator(
    task_id='copy_to_shared_folder',
    bash_command=copy_to_shared_folder_cmd,
    execution_timeout=timedelta(hours=3),
    pool='data_transfers',
    provide_context=True,
    params={'dest':shared_data_folder},
    dag=dag)
copy_to_shared_folder.set_upstream(extract_dicom_info)

copy_to_shared_folder.doc_md = """\
# Copy data to shared folder

"""

test_spm = SpmOperator(
    task_id='test_spm',
    python_callable=spmTest,
    dag=dag
    )

test_spm.set_upstream(copy_to_shared_folder)
