"""

Pre-process DICOM files in a study folder

"""

import logging
import os

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow_spm.operators import SpmPipelineOperator
from airflow_pipeline.operators import PythonPipelineOperator

from mri_meta_extract.files_recording import create_provenance
from mri_meta_extract.files_recording import visit

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


def pre_process_dicom_dag(dataset, dataset_section, email_errors_to, max_active_runs, misc_library_path,
                          dataset_config=None, copy_to_local=True, images_organizer=False,
                          images_selection=False, dicom_select_t1=False, protocols_file=None,  mpm_maps=True,
                          mpm_maps_spm_function='Preproc_mpm_maps', mpm_maps_pipeline_path=None,
                          mpm_maps_local_folder=None, mpm_maps_server_folder=None, neuro_morphometric_atlas=True,
                          neuro_morphometric_atlas_spm_function='NeuroMorphometric_pipeline',
                          neuro_morphometric_atlas_pipeline_path=None, neuro_morphometric_atlas_local_folder=None,
                          neuro_morphometric_atlas_server_folder=None,
                          neuro_morphometric_atlas_tpm_template='nwTPM_sl3.nii', import_features_local_folder=None):

    # functions used in the DAG

    def extract_images_info_fn(folder, session_id, step_name, software_versions=None, **kwargs):
        """
         Extract the information from DICOM/NIFTI files located inside a folder.
         The folder information should be given in the configuration parameter
         'folder' of the DAG run
        """
        logging.info('folder %s, session_id %s', folder, session_id)

        provenance = create_provenance(dataset, software_versions=software_versions)
        step = visit(folder, provenance, step_name, config=dataset_config)

        return step

    def neuro_morphometric_atlas_arguments_fn(folder, session_id, **kwargs):
        """
          Prepare the arguments for the pipeline that builds a Neuro morphometric
          atlas from the Nitfi files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')
        table_format = 'csv'

        return [session_id,
                parent_data_folder,
                neuro_morphometric_atlas_local_folder,
                neuro_morphometric_atlas_server_folder,
                protocols_file,
                table_format,
                neuro_morphometric_atlas_tpm_template]

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

    dicom_to_nifti_success, upstream_step = dicom_to_nifti_pipeline_cfg(dag, upstream_step, dataset_section)

    if copy_to_local:
        copy_step = cleanup_local_cfg(dag, upstream_step, dataset_section, "DICOM_LOCAL_FOLDER")
        upstream_step.priority_weight = copy_step.priority_weight
    # endif

    upstream = upstream_step.task
    upstream_id = upstream_step.task_id
    priority_weight = upstream_step.priority_weight

    if mpm_maps:
        upstream, upstream_id, priority_weight = mpm_maps_pipeline_cfg(dag, upstream, upstream_id, priority_weight,
                                                                       dataset_section)

    # endif

    if neuro_morphometric_atlas:

        neuro_morphometric_atlas_pipeline = SpmPipelineOperator(
            task_id='neuro_morphometric_atlas_pipeline',
            spm_function=neuro_morphometric_atlas_spm_function,
            spm_arguments_callable=neuro_morphometric_atlas_arguments_fn,
            matlab_paths=[misc_library_path,
                          neuro_morphometric_atlas_pipeline_path,
                          mpm_maps_pipeline_path],
            output_folder_callable=lambda session_id, **kwargs: (neuro_morphometric_atlas_local_folder + '/' +
                                                                 session_id),
            pool='image_preprocessing',
            parent_task=upstream_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=24),
            on_skip_trigger_dag_id='mri_notify_skipped_processing',
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            dataset_config=dataset_config,
            dag=dag
        )
        neuro_morphometric_atlas_pipeline.set_upstream(upstream)

        neuro_morphometric_atlas_pipeline.doc_md = dedent("""\
        # NeuroMorphometric Pipeline

        SPM function: __%s__

        This function computes an individual Atlas based on the NeuroMorphometrics Atlas. This is based on the
        NeuroMorphometrics Toolbox.
        This delivers three files:

        1. Atlas File (*.nii);
        2. Volumes of the Morphometric Atlas structures (*.txt);
        3. CSV File (.csv) containing the volume, globals, and Multiparametric Maps (R2*, R1, MT, PD) for each
        structure defined in the Subject Atlas.

        The atlas is calculated locally and finally copied to a remote folder:

        * Local folder: %s
        * Remote folder: %s

        Depends on: __%s__
        """ % (neuro_morphometric_atlas_spm_function, neuro_morphometric_atlas_local_folder,
               neuro_morphometric_atlas_server_folder, upstream_id))

        upstream = neuro_morphometric_atlas_pipeline
        upstream_id = "neuro_morphometric_atlas_pipeline"
        priority_weight += 5

        extract_nifti_atlas_info = PythonPipelineOperator(
            task_id='extract_nifti_atlas_info',
            python_callable=extract_images_info_fn,
            parent_task='neuro_morphometric_atlas_pipeline',
            pool='io_intensive',
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=3),
            dag=dag
        )

        extract_nifti_atlas_info.set_upstream(
            neuro_morphometric_atlas_pipeline)

        extract_nifti_atlas_info.doc_md = dedent("""\
        # Extract information from NIFTI files generated by Neuro Morphometrics Atlas pipeline

        Read NIFTI information from directory %s containing the Nifti files created by Neuro Morphometrics Atlas
        pipeline and store that information in the database.
        """ % neuro_morphometric_atlas_local_folder)

        dicom_to_nifti_success.set_upstream(extract_nifti_atlas_info)
        priority_weight += 5

        import_features_pipeline = PythonPipelineOperator(
            task_id='import_features_pipeline',
            python_callable=features_to_i2b2_fn,
            output_folder_callable=lambda session_id, **kwargs: import_features_local_folder + '/' + session_id,
            pool='io_intensive',
            parent_task=upstream_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=6),
            on_skip_trigger_dag_id='mri_notify_skipped_processing',
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            dag=dag
        )

        import_features_pipeline.set_upstream(upstream)

        import_features_pipeline.doc_md = dedent("""\
        # import brain features from CSV files to I2B2 DB

        Read CSV files containing brain features and import it into an I2B2 DB.

        Depends on: __%s__
        """ % upstream_id)

    # endif

    return dag
