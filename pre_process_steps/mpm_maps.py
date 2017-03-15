"""

  Pre processing step: MPM Maps

  Configuration variables used:

  * DATASET_CONFIG
  * PIPELINES_PATH
  * MPM_MAPS_SPM_FUNCTION
  * MPM_MAPS_LOCAL_FOLDER

"""

import os

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_spm.operators import SpmPipelineOperator

from common_steps import Step, default_config


def mpm_maps_pipeline_cfg(dag, upstream_step, dataset_section):
    default_config(dataset_section, 'DATASET_CONFIG', '')
    default_config(dataset_section, 'MPM_MAPS_SPM_FUNCTION', 'selectT1')

    dataset_config = configuration.get(dataset_section, 'DATASET_CONFIG')
    pipeline_path = configuration.get(dataset_section, 'PIPELINES_PATH') + '/MPMs_Pipeline'
    misc_library_path = configuration.get(dataset_section, 'PIPELINES_PATH') + '/../Miscellaneous&Others'
    spm_function = configuration.get(dataset_section, 'MPM_MAPS_SPM_FUNCTION')
    local_folder = configuration.get(dataset_section, 'MPM_MAPS_LOCAL_FOLDER')

    return mpm_maps_pipeline(dag, upstream_step,
                             dataset_config=dataset_config,
                             pipeline_path=pipeline_path,
                             misc_library_path=misc_library_path,
                             spm_function=spm_function,
                             local_folder=local_folder)


def mpm_maps_pipeline(dag, upstream_step,
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

    mpm_maps_pipeline = SpmPipelineOperator(
        task_id='mpm_maps_pipeline',
        spm_function=spm_function,
        spm_arguments_callable=arguments_fn,
        matlab_paths=[misc_library_path, pipeline_path],
        output_folder_callable=lambda session_id, **kwargs: local_folder + '/' + session_id,
        pool='image_preprocessing',
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=24),
        on_skip_trigger_dag_id='mri_notify_skipped_processing',
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag
    )

    mpm_maps_pipeline.set_upstream(upstream_step.task)

    mpm_maps_pipeline.doc_md = dedent("""\
        # MPM Maps Pipeline

        SPM function: __%s__

        This function computes the Multiparametric Maps (MPMs) (R2*, R1, MT, PD) and brain segmentation in different
        tissue maps.
        All computation was programmed based on the LREN database structure.

        The MPMs are calculated locally and finally copied to a remote folder:

        * Local folder: __%s__

        Depends on: __%s__
        """ % (spm_function, local_folder, upstream_step.task_id))

    return Step(mpm_maps_pipeline, 'mpm_maps_pipeline', upstream_step.priority_weight + 10)
