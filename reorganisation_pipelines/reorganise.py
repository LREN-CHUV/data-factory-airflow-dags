"""Reorganise files in oder to match the pre-processing pipelines requirements"""

from datetime import datetime, timedelta

from airflow import DAG

from common_steps import initial_step
from common_steps.check_local_free_space import check_local_free_space_cfg
from common_steps.prepare_pipeline import prepare_pipeline
from common_steps.cleanup_local import cleanup_local_cfg
from reorganisation_steps.copy_all_to_local import copy_all_to_local_cfg
from reorganisation_steps.reorganise import reorganise_cfg
from reorganisation_steps.trigger_preprocessing import trigger_preprocessing_cfg
from reorganisation_steps.trigger_ehr import trigger_ehr_cfg


preparation_steps = ['copy_all_to_local']
reorganisation_steps = ['dicom_reorganise', 'nifti_reorganise']
finalisation_steps = ['trigger_preprocessing', 'trigger_ehr']

steps_with_file_outputs = preparation_steps + reorganisation_steps


def reorganise_dag(dataset, section, email_errors_to, max_active_runs, reorganisation_pipelines=''):

    # Define the DAG

    dag_name = '%s_reorganise' % dataset.lower().replace(" ", "_")

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.now(),
        'retries': 1,
        'retry_delay': timedelta(seconds=120),
        'email': email_errors_to,
        'email_on_failure': True,
        'email_on_retry': True
    }

    dag = DAG(
        dag_id=dag_name,
        default_args=default_args,
        schedule_interval=None,
        max_active_runs=max_active_runs)

    upstream_step = check_local_free_space_cfg(dag, initial_step, section,
                                               map(lambda p: section + ':' + p, steps_with_file_outputs))

    upstream_step = prepare_pipeline(dag, upstream_step, True)
    upstream_step = copy_all_to_local_cfg(dag, upstream_step, section, section + ':copy_all_to_local')

    if 'dicom_reorganise' in reorganisation_pipelines:
        upstream_step = reorganise_cfg(dag, upstream_step, section, section + ':dicom_reorganise')
    elif 'nifti_reorganise' in reorganisation_pipelines:
        upstream_step = reorganise_cfg(dag, upstream_step, section, section + ':nifti_reorganise')

    copy_step = cleanup_local_cfg(dag, upstream_step, section + ':copy_all_to_local')
    upstream_step.priority_weight = copy_step.priority_weight

    if 'trigger_preprocessing' in reorganisation_pipelines:
        upstream_step = trigger_preprocessing_cfg(dag, upstream_step, section, section + ':trigger_preprocessing')
    # endif

    if 'trigger_ehr' in reorganisation_pipelines:
        upstream_step = trigger_ehr_cfg(dag, upstream_step, section, section + ':trigger_ehr')
    # endif

    # notify_success(dag, upstream_step)

    return dag
