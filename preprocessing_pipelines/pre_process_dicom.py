"""

Pre-process DICOM files in a study folder

"""

from datetime import datetime, timedelta

from airflow import DAG

from common_steps import initial_step
from common_steps.prepare_pipeline import prepare_pipeline
from common_steps.check_free_space_local import check_free_space_local_cfg

from preprocessing_steps.cleanup_local import cleanup_local_cfg
from preprocessing_steps.copy_to_local import copy_to_local_cfg
from preprocessing_steps.register_local import register_local_cfg
from preprocessing_steps.images_organizer import images_organizer_cfg
from preprocessing_steps.images_selection import images_selection_pipeline_cfg
from preprocessing_steps.dicom_select_t1 import dicom_select_t1_pipeline_cfg
from preprocessing_steps.dicom_to_nifti import dicom_to_nifti_pipeline_cfg
from preprocessing_steps.mpm_maps import mpm_maps_pipeline_cfg
from preprocessing_steps.neuro_morphometric_atlas import neuro_morphometric_atlas_pipeline_cfg
from preprocessing_steps.notify_success import notify_success

from etl_steps.features_to_i2b2 import features_to_i2b2_pipeline_cfg


def pre_process_dicom_dag(dataset, dataset_section, email_errors_to, max_active_runs, preprocessing_pipelines=''):

    # Define the DAG

    dag_name = '%s_mri_pre_process_dicom' % dataset.lower().replace(" ", "_")

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

    upstream_step = check_free_space_local_cfg(dag, initial_step, dataset_section, "DICOM_LOCAL_FOLDER")

    upstream_step = prepare_pipeline(dag, upstream_step, True)

    copy_to_local = 'copy_to_local' in preprocessing_pipelines
    images_organizer = 'dicom_organizer' in preprocessing_pipelines
    dicom_select_t1 = 'dicom_select_T1' in preprocessing_pipelines
    images_selection = 'images_selection' in preprocessing_pipelines
    mpm_maps = 'mpm_maps' in preprocessing_pipelines
    neuro_morphometric_atlas = 'neuro_morphometric_atlas' in preprocessing_pipelines
    export_features = 'export_features' in preprocessing_pipelines

    if copy_to_local:
        upstream_step = copy_to_local_cfg(dag, upstream_step, dataset_section, "DICOM_LOCAL_FOLDER")
    else:
        upstream_step = register_local_cfg(dag, upstream_step, dataset_section)
    # endif

    if images_organizer:
        upstream_step = images_organizer_cfg(dag, upstream_step, dataset_section, dataset, "DICOM_LOCAL_FOLDER")
    # endif

    if images_selection:
        upstream_step = images_selection_pipeline_cfg(dag, upstream_step, dataset_section)
    # endif

    if dicom_select_t1:
        upstream_step = dicom_select_t1_pipeline_cfg(dag, upstream_step, dataset_section)
    # endif

    upstream_step = dicom_to_nifti_pipeline_cfg(dag, upstream_step, dataset_section)

    if copy_to_local:
        copy_step = cleanup_local_cfg(dag, upstream_step, dataset_section, "DICOM_LOCAL_FOLDER")
        upstream_step.priority_weight = copy_step.priority_weight
    # endif

    if mpm_maps:
        upstream_step = mpm_maps_pipeline_cfg(dag, upstream_step, dataset_section)
    # endif

    if neuro_morphometric_atlas:
        upstream_step = neuro_morphometric_atlas_pipeline_cfg(dag, upstream_step, dataset_section)
        if export_features:
            upstream_step = features_to_i2b2_pipeline_cfg(dag, upstream_step, dataset_section)
        # endif
    # endif

    notify_success(dag, upstream_step)

    return dag
