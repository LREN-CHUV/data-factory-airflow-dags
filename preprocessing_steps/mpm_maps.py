"""

Pre processing step: MPM Maps.

Computes the Multiparametric Maps (MPMs)(R2*, R1, MT, PD) and brain segmentation
in different tissue maps.

Configuration variables used:

* :preprocessing section
    * INPUT_CONFIG: List of flags defining how incoming imaging data are organised.
    * PIPELINES_PATH: Path to the root folder containing the Matlab scripts for the pipelines.
* :preprocessing:mpm_maps section
    * OUTPUT_FOLDER: destination folder for the MPMs and brain segmentation
    * BACKUP_FOLDER: backup folder for the MPMs and brain segmentation
    * SPM_FUNCTION: SPM function called. Default to 'Preproc_mpm_maps'
    * PIPELINE_PATH: path to the folder containing the SPM script for this pipeline.
      Default to PIPELINES_PATH + '/MPMs_Pipeline'
    * MISC_LIBRARY_PATH: path to the Misc&Libraries folder for SPM pipelines.
      Default to MISC_LIBRARY_PATH value in [data-factory:&lt;dataset&gt;:preprocessing] section.
    * PROTOCOLS_DEFINITION_FILE: path to the Protocols definition file defining the protocols used on the scanner.
      Default to PROTOCOLS_DEFINITION_FILE value in [data-factory:&lt;dataset&gt;:preprocessing] section.

"""

import os

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_spm.operators import SpmPipelineOperator

from common_steps import Step, default_config


def mpm_maps_pipeline_cfg(dag, upstream_step, preprocessing_section, step_section):
    default_config(preprocessing_section, 'INPUT_CONFIG', '')
    default_config(preprocessing_section, 'PIPELINES_PATH', '.')
    default_config(step_section, 'SPM_FUNCTION', 'Preproc_mpm_maps')
    default_config(step_section, 'PIPELINE_PATH', configuration.get(
        preprocessing_section, 'PIPELINES_PATH') + '/MPMs_Pipeline')
    default_config(step_section, 'MISC_LIBRARY_PATH', configuration.get(preprocessing_section, 'MISC_LIBRARY_PATH'))
    default_config(step_section, 'PROTOCOLS_DEFINITION_FILE',
                   configuration.get(step_section, 'PROTOCOLS_FILE'))

    dataset_config = configuration.get(preprocessing_section, 'INPUT_CONFIG')
    pipeline_path = configuration.get(step_section, 'PIPELINE_PATH')
    misc_library_path = configuration.get(step_section, 'MISC_LIBRARY_PATH')
    spm_function = configuration.get(step_section, 'SPM_FUNCTION')
    output_folder = configuration.get(step_section, 'OUTPUT_FOLDER')
    backup_folder = configuration.get(step_section, 'BACKUP_FOLDER')
    protocols_definition_file = configuration.get(step_section, 'PROTOCOLS_DEFINITION_FILE')

    return mpm_maps_pipeline(dag, upstream_step,
                             dataset_config=dataset_config,
                             pipeline_path=pipeline_path,
                             misc_library_path=misc_library_path,
                             spm_function=spm_function,
                             output_folder=output_folder,
                             backup_folder=backup_folder,
                             protocols_definition_file=protocols_definition_file)


def mpm_maps_pipeline(dag, upstream_step,
                      dataset_config='',
                      spm_function='Preproc_mpm_maps',
                      pipeline_path=None,
                      misc_library_path=None,
                      output_folder=None,
                      backup_folder=None,
                      protocols_definition_file=None):

    def arguments_fn(folder, session_id, pipeline_params_config_file='Preproc_mpm_maps_pipeline_config.txt', **kwargs):
        """
          Prepare the arguments for the pipeline that selects T1 files from DICOM.
          It selects all T1 files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')

        return [parent_data_folder,
                session_id,
                output_folder,
                protocols_definition_file,
                pipeline_params_config_file,
                backup_folder]

    mpm_maps_pipeline = SpmPipelineOperator(
        task_id='mpm_maps_pipeline',
        spm_function=spm_function,
        spm_arguments_callable=arguments_fn,
        matlab_paths=[misc_library_path, pipeline_path],
        output_folder_callable=lambda session_id, **kwargs: output_folder + '/' + session_id,
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=24),
        pool='image_preprocessing',
        on_skip_trigger_dag_id='mri_notify_skipped_processing',
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag
    )

    mpm_maps_pipeline.set_upstream(upstream_step.task)

    mpm_maps_pipeline.doc_md = dedent("""\
            # MPM Maps Pipeline

            SPM function: __%s__

            This function computes the Multiparametric Maps (MPMs) (R2*, R1, MT, PD) and brain segmentation in
            different tissue maps.
            All computation was programmed based on the LREN database structure.

            The MPMs are calculated locally and finally copied to a remote folder:

            * Local folder: __%s__
            * Remote folder: __%s__

            Depends on: __%s__
            """ % (spm_function, output_folder, backup_folder, upstream_step.task_id))

    return Step(mpm_maps_pipeline, 'mpm_maps_pipeline', upstream_step.priority_weight + 10)
