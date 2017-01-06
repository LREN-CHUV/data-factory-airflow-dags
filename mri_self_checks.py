"""

Runs self-checks

"""

import os

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow_spm.operators import SpmOperator
from airflow_freespace.operators import FreeSpaceSensor
from airflow import configuration


# constants

DAG_NAME = 'mri_self_checks'

spm_config_folder = configuration.get('spm', 'SPM_DIR')
dataset_sections = configuration.get('mri', 'DATASETS')

# functions


def check_python_fn():
    import socket
    print("Hostname: %s" % socket.gethostname())
    print("Environement:")
    print("-------------")
    for k, v in os.environ.items():
        print("%s = %s" % (k, v))
    print("-------------")


def check_spm_fn(engine):
    print("Checking Matlab...")
    ret = engine.sqrt(4.0)
    if int(ret) != 2:
        raise RuntimeError("Matlab integration is not working") from error
    print("sqrt(4) = %s" % ret)
    print("[OK]")
    print("Checking SPM...")
    spm_dir = engine.spm('Dir')
    if spm_dir != spm_config_folder:
        raise RuntimeError("SPM integration is not working, found SPM in directory %s" % spm_dir) from error
    print("[OK]")


# Define the DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(seconds=120),
    'email': 'ludovic.claude@chuv.ch',
    'email_on_failure': True,
    'email_on_retry': True
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval='@once')

check_python = PythonOperator(
    task_id='check_python',
    python_callable=check_python_fn,
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

check_python.doc_md = """\
# Check Python and its environment

Displays some technical information about the Python runtime.
"""


check_spm = SpmOperator(
    task_id='spm_check',
    python_callable=check_spm_fn,
    matlab_paths=[],
    execution_timeout=timedelta(minutes=10),
    dag=dag
)

check_spm.set_upstream(check_python)

check_spm.doc_md = """\
# Check SPM

Checks that SPM is running as expected.
"""

for dataset_section in dataset_sections.split(','):
    dataset = configuration.get(dataset_section, 'DATASET')
    min_free_space_local_folder = configuration.getfloat(
        dataset_section, 'MIN_FREE_SPACE_LOCAL_FOLDER')
    dicom_local_folder = configuration.get(
        dataset_section, 'DICOM_LOCAL_FOLDER')

    check_free_space = FreeSpaceSensor(
        task_id='%s_check_free_space' % dataset.lower().replace(" ", "_"),
        path=local_drive,
        free_disk_threshold=min_free_space_local_folder,
        retry_delay=timedelta(hours=1),
        retries=24 * 7,
        dag=dag
    )

    check_free_space.set_upstream(check_spm)

    check_free_space.doc_md = dedent("""\
    # Check free space

    Check that there is enough free space on the local drive for processing dataset %s, wait otherwise.
    """ % dataset)
