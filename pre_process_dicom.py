"""

Pre-process DICOM files in a study folder

"""

import logging
  
from datetime import datetime, timedelta, time
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.models import Variable

# constants

DAG_NAME = 'pre_process_dicom'

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

def run_this_func(ds, **kwargs):
    print("Remotely received value of {} for key=folder".format(kwargs['dag_run'].conf['folder']))
    print("Remotely received value of {} for key=session_id".format(kwargs['dag_run'].conf['session_id']))

run_this = PythonOperator(
    task_id='run_this',
    provide_context=True,
    python_callable=run_this_func,
    dag=dag)
