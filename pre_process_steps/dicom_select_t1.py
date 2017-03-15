"""

  Pre processing step: Select T1 DICOM files

  Configuration variables used:

  * DATASET_CONFIG
  * PIPELINES_PATH
  * DICOM_SELECT_T1_SPM_FUNCTION
  * DICOM_SELECT_T1_LOCAL_FOLDER
  * DICOM_SELECT_T1_PROTOCOLS_FILE

"""

import os

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_spm.operators import SpmPipelineOperator

from common_steps import Step, default_config


def dicom_select_t1_pipeline_cfg(dag, upstream_step, dataset_section):
    default_config(dataset_section, 'DATASET_CONFIG', '')
    default_config(dataset_section, 'DICOM_SELECT_T1_SPM_FUNCTION', 'selectT1')

    dataset_config = configuration.get(dataset_section, 'DATASET_CONFIG')
    pipeline_path = configuration.get(dataset_section, 'PIPELINES_PATH') + '/SelectT1_Pipeline'
    misc_library_path = configuration.get(dataset_section, 'PIPELINES_PATH') + '/../Miscellaneous&Others'
    spm_function = configuration.get(dataset_section, 'DICOM_SELECT_T1_SPM_FUNCTION')
    local_folder = configuration.get(dataset_section, 'DICOM_SELECT_T1_LOCAL_FOLDER')
    protocols_file = configuration.get(dataset_section, 'DICOM_SELECT_T1_PROTOCOLS_FILE')

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
