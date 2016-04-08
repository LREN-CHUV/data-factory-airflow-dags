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
import preprocess_dicom
import pprint
import os, sys

from datetime import datetime, timedelta, time
from airflow import DAG
from airflow.operators import BashOperator, PythonOperator, TriggerDagRunOperator
from airflow.models import Variable

# constants
  
START = datetime.combine(datetime.today() - timedelta(days=2), datetime.min.time()) + timedelta(hours=10)
DAG_NAME = 'poll_pre_processing_incoming'

# functions

def dispatch(**context):
    dirs = context['task_instance'].xcom_pull('scan_dirs_ready_for_preprocessing')
    


pp = pprint.PrettyPrinter(indent=4)

def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""
    c_p =context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message'], 'folder': context['params']['folder']}
        pp.pprint(dag_run_obj.payload)
        return dag_run_obj

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
          schedule_interval='0 1 * * *')

preprocessing_data_folder = Variable.get("preprocessing_data_folder")

scan_ready_dirs_command_template = """

    for d in $(find {{ preprocessing_data_folder }} -maxdepth 1 -type d) ; do
      if [ -f .ready ] ; then
        echo $(basename $(dirname))
      fi
    done

"""

scan_ready_dirs = BashOperator(
    task_id='scan_dirs_ready_for_preprocessing',
    bash_command=scan_ready_dirs_command_template,
    params={'preprocessing_data_folder': preprocessing_data_folder},
    xcom_push=True,
    dag=dag)

dispatch_ready_dirs = PythonOperator(
    task_id='dispatch_dirs_ready_for_preprocessing',
    python_callable=run_this_func,
    provide_context=True,
    dag=dag)

# Define the single task in this controller example DAG
trigger = TriggerDagRunOperator(task_id='test_trigger_dagrun',
                                trigger_dag_id=preprocess_dicom.DAG_NAME,
                                python_callable=conditionally_trigger,
                                params={'condition_param': True,
                                        'message': 'Hello World',
                                        'folder': preprocessing_data_folder},
                                dag=dag)
