"""

Pre-process DICOM files in a study folder

"""

from datetime import datetime, timedelta

from airflow import DAG

from i2b2_import import features_csv_import

from common_steps import initial_step
from common_steps.prepare_pipeline import prepare_pipeline

from pre_process_steps.check_free_space_local import check_free_space_local_cfg
from pre_process_steps.cleanup_local import cleanup_local_cfg
from pre_process_steps.copy_to_local import copy_to_local_cfg
from pre_process_steps.register_local import register_local_cfg
from pre_process_steps.images_organizer import images_organizer_cfg
from pre_process_steps.images_selection import images_selection_pipeline_cfg
from pre_process_steps.dicom_select_t1 import dicom_select_t1_pipeline_cfg
from pre_process_steps.dicom_to_nifti import dicom_to_nifti_pipeline_cfg
from pre_process_steps.mpm_maps import mpm_maps_pipeline_cfg
from pre_process_steps.neuro_morphometric_atlas import neuro_morphometric_atlas_pipeline_cfg
from pre_process_steps.notify_success import notify_success


def pre_process_dicom_dag(dataset, dataset_section, email_errors_to, max_active_runs,
                          dataset_config=None, preprocessing_pipelines=''):

    # functions used in the DAG

    def features_to_i2b2_fn(folder, i2b2_conn, **kwargs):
        """
          Import brain features from CSV files to I2B2 DB
        """
        features_csv_import.folder2db(folder, i2b2_conn, dataset, dataset_config)

        return "ok"

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

#        import_features_pipeline = PythonPipelineOperator(
#            task_id='import_features_pipeline',
#            python_callable=features_to_i2b2_fn,
#            output_folder_callable=lambda session_id, **kwargs: import_features_local_folder + '/' + session_id,
#            pool='io_intensive',
#            parent_task=upstream_id,
#            priority_weight=priority_weight,
#            execution_timeout=timedelta(hours=6),
#            on_skip_trigger_dag_id='mri_notify_skipped_processing',
#            on_failure_trigger_dag_id='mri_notify_failed_processing',
#            dag=dag
#        )
#
#        import_features_pipeline.set_upstream(upstream)
#
#        import_features_pipeline.doc_md = dedent("""\
#        # import brain features from CSV files to I2B2 DB
#
#        Read CSV files containing brain features and import it into an I2B2 DB.
#
#        Depends on: __%s__
#        """ % upstream_id)

    # endif

    notify_success(dag, upstream_step)

    return dag
