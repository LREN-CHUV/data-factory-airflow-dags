"""Runs self-checks"""

import os
import logging

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.operators.python_operator import PythonOperator
from airflow_spm.operators import SpmOperator
from airflow_freespace.operators import FreeSpaceSensor
from airflow import configuration


# constants

DAG_NAME = 'mri_self_checks'

spm_config_folder = configuration.get('spm', 'SPM_DIR')
dataset_sections = configuration.get('data-factory', 'DATASETS')

# functions


def check_python_fn():
    import socket
    logging.info("Hostname: %s", socket.gethostname())
    logging.info("Environement:")
    logging.info("-------------")
    for k, v in os.environ.items():
        logging.info("%s = %s", k, v)
    logging.info("-------------")


def check_spm_fn(engine):
    logging.info("Checking Matlab...")
    ret = engine.sqrt(4.0)
    if int(ret) != 2:
        raise RuntimeError("Matlab integration is not working")
    logging.info("sqrt(4) = %s", ret)
    logging.info("[OK]")
    logging.info("Checking SPM...")
    spm_dir = engine.spm('Dir')
    if spm_dir != spm_config_folder:
        raise RuntimeError("SPM integration is not working, found SPM in directory %s" % spm_dir)
    logging.info("[OK]")


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

# Run the self-checks hourly to keep some activity and make the scheduler health-check happy
dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval='@hourly')

latest_only = LatestOnlyOperator(
    task_id='latest_only',
    dag=dag
)

latest_only.doc_md = """\
# Skip old checks

Old checks are skipped and only the most recent run is used for the self-checks.
"""

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

check_python.set_upstream(latest_only)


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

for dataset in dataset_sections.split(','):
    dataset_section = 'data-factory:%s' % dataset

    dataset_name = configuration.get(dataset_section, 'DATASET_LABEL')
    min_free_space_local_folder = configuration.getfloat(
        dataset_section + ':preprocessing', 'MIN_FREE_SPACE')
    dicom_local_folder = configuration.get(
        dataset_section + ':preprocessing:copy_to_local', 'OUTPUT_FOLDER')

    check_free_space = FreeSpaceSensor(
        task_id='%s_check_free_space' % dataset_name.lower().replace(" ", "_"),
        path=dicom_local_folder,
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
