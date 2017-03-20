"""

  Pre processing step: Select T1 DICOM files

  Configuration variables used:

  * :preprocessing section
    * INPUT_CONFIG
  * :preprocessing:dicom_select_T1 section
    * OUTPUT_FOLDER
    * SPM_FUNCTION
    * PROTOCOLS_FILE
    * PIPELINE_PATH
    * MISC_LIBRARY_PATH

"""

import os

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_spm.operators import SpmPipelineOperator

from common_steps import Step, default_config


def dicom_select_t1_pipeline_cfg(dag, upstream_step, preprocessing_section, step_section):
    default_config(preprocessing_section, 'DATASET_CONFIG', '')
    default_config(step_section, 'SPM_FUNCTION', 'selectT1')
    default_config(step_section, 'MISC_LIBRARY_PATH', configuration.get(preprocessing_section, 'MISC_LIBRARY_PATH'))

    dataset_config = configuration.get(preprocessing_section, 'INPUT_CONFIG')
    pipeline_path = configuration.get(step_section, 'PIPELINE_PATH')
    misc_library_path = configuration.get(step_section, 'MISC_LIBRARY_PATH')
    spm_function = configuration.get(step_section, 'SPM_FUNCTION')
    local_folder = configuration.get(step_section, 'OUTPUT_FOLDER')
    protocols_file = configuration.get(step_section, 'PROTOCOLS_FILE')

    return dicom_select_t1_pipeline(dag, upstream_step,
                                    dataset_config=dataset_config,
                                    pipeline_path=pipeline_path,
                                    misc_library_path=misc_library_path,
                                    spm_function=spm_function,
                                    local_folder=local_folder,
                                    protocols_file=protocols_file)


def dicom_select_t1_pipeline(dag, upstream_step,
                             dataset_config=None,
                             spm_function='selectT1',
                             pipeline_path=None,
                             misc_library_path=None,
                             local_folder=None,
                             protocols_file=None):

    def arguments_fn(folder, session_id, **kwargs):
        """
          Prepare the arguments for the pipeline that selects T1 files from DICOM.
          It selects all T1 files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')

        return [parent_data_folder,
                local_folder,
                session_id,
                protocols_file]

    dicom_select_t1_pipeline = SpmPipelineOperator(
        task_id='dicom_select_T1_pipeline',
        spm_function=spm_function,
        spm_arguments_callable=arguments_fn,
        matlab_paths=[misc_library_path, pipeline_path],
        output_folder_callable=lambda session_id, **kwargs: local_folder + '/' + session_id,
        pool='io_intensive',
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=24),
        on_skip_trigger_dag_id='mri_notify_skipped_processing',
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag
    )

    if upstream_step.task:
        dicom_select_t1_pipeline.set_upstream(upstream_step.task)

    dicom_select_t1_pipeline.doc_md = dedent("""\
        # select T1 DICOM pipeline

        SPM function: __%s__

        Selects only T1 images from a set of various DICOM images.

        Selected DICOM files are stored the the following locations:

        * Local folder: __%s__

        Depends on: __%s__
        """ % (spm_function, local_folder, upstream_step.task_id))

    return Step(dicom_select_t1_pipeline, 'dicom_select_T1_pipeline', upstream_step.priority_weight + 10)
