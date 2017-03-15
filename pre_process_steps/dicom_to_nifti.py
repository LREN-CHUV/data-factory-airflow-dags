"""

  Pre processing step: DICOM to Nifti conversion

  Configuration variables used:

  * DATASET_CONFIG
  * PIPELINES_PATH
  * NIFTI_SPM_FUNCTION
  * NIFTI_LOCAL_FOLDER
  * NIFTI_SERVER_FOLDER
  * PROTOCOLS_FILE
  * DCM2NII_PROGRAM

"""

import os

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow.operators import TriggerDagRunOperator

from airflow_pipeline.pipelines import pipeline_trigger
from airflow_spm.operators import SpmPipelineOperator

from common_steps import default_config


def dicom_to_nifti_pipeline_cfg(dag, upstream, upstream_id, priority_weight, dataset_section):
    default_config(dataset_section, 'DATASET_CONFIG', '')
    default_config(dataset_section, 'NIFTI_SPM_FUNCTION', 'DCM2NII_LREN')

    dataset_config = configuration.get(dataset_section, 'DATASET_CONFIG')
    pipeline_path = configuration.get(dataset_section, 'PIPELINES_PATH') + '/Nifti_Conversion_Pipeline'
    misc_library_path = configuration.get(dataset_section, 'PIPELINES_PATH') + '/../Miscellaneous&Others'
    spm_function = configuration.get(dataset_section, 'NIFTI_SPM_FUNCTION')
    local_folder = configuration.get(dataset_section, 'NIFTI_LOCAL_FOLDER')
    server_folder = configuration.get(dataset_section, 'NIFTI_SERVER_FOLDER')
    protocols_file = configuration.get(dataset_section, 'PROTOCOLS_FILE')
    dcm2nii_program = configuration.get(dataset_section, 'DCM2NII_PROGRAM')

    return dicom_to_nifti_pipeline(dag, upstream, upstream_id, priority_weight,
                                   dataset_config=dataset_config,
                                   pipeline_path=pipeline_path,
                                   misc_library_path=misc_library_path,
                                   spm_function=spm_function,
                                   local_folder=local_folder,
                                   server_folder=server_folder,
                                   protocols_file=protocols_file,
                                   dcm2nii_program=dcm2nii_program)


def dicom_to_nifti_pipeline(dag, upstream, upstream_id, priority_weight,
                            dataset='',
                            dataset_config='',
                            spm_function='DCM2NII_LREN',
                            pipeline_path=None,
                            misc_library_path=None,
                            local_folder=None,
                            server_folder=None,
                            protocols_file=None,
                            dcm2nii_program=None):

    def arguments_fn(folder, session_id, **kwargs):
        """
          Prepare the arguments for conversion pipeline from DICOM to Nifti format.
          It converts all files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')

        return [parent_data_folder,
                session_id,
                local_folder,
                server_folder,
                protocols_file,
                dcm2nii_program]

    dicom_to_nifti_pipeline = SpmPipelineOperator(
        task_id='dicom_to_nifti_pipeline',
        spm_function=spm_function,
        spm_arguments_callable=arguments_fn,
        matlab_paths=[misc_library_path, pipeline_path],
        output_folder_callable=lambda session_id, **kwargs: local_folder + '/' + session_id,
        pool='io_intensive',
        parent_task=upstream_id,
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=24),
        on_skip_trigger_dag_id='mri_notify_skipped_processing',
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag
    )

    dicom_to_nifti_pipeline.set_upstream(upstream)

    dicom_to_nifti_pipeline.doc_md = dedent("""\
    # DICOM to Nitfi Pipeline

    SPM function: __%s__

    This function convert the dicom files to Nifti format using the SPM tools and
    [dcm2nii](http://www.mccauslandcenter.sc.edu/mricro/mricron/dcm2nii.html) tool developed by Chris Rorden.

    Nifti files are stored the the following locations:

    * Local folder: __%s__
    * Remote folder: __%s__

    Depends on: __%s__
    """ % (spm_function, local_folder, server_folder, upstream_id))

    upstream = dicom_to_nifti_pipeline
    upstream_id = 'dicom_to_nifti_pipeline'
    priority_weight += 10

    notify_success = TriggerDagRunOperator(
        task_id='notify_success',
        trigger_dag_id='mri_notify_successful_processing',
        python_callable=pipeline_trigger('extract_nifti_atlas_info'),
        priority_weight=999,
        dag=dag
    )

    notify_success.set_upstream(upstream)

    notify_success.doc_md = dedent("""\
    # Notify successful processing

    Notify successful processing of this MRI scan session.
    """)

    return notify_success, upstream, upstream_id, priority_weight
