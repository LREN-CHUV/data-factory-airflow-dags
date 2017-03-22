"""

Pre processing step: DICOM to Nifti conversion.

Convert all DICOM files to Nifti format.

Configuration variables used:

* :preprocessing section
    * INPUT_CONFIG: List of flags defining how incoming imaging data are organised.
    * PIPELINES_PATH: Path to the root folder containing the Matlab scripts for the pipelines.
* :preprocessing:dicom_select_T1 section
    * OUTPUT_FOLDER: destination folder for the Nitfi images
    * BACKUP_FOLDER: backup folder for the Nitfi images
    * SPM_FUNCTION: SPM function called. Default to 'DCM2NII_LREN'
    * PIPELINE_PATH: path to the folder containing the SPM script for this pipeline.
      Default to PIPELINES_PATH + '/Nifti_Conversion_Pipeline'
    * MISC_LIBRARY_PATH: path to the Misc&Libraries folder for SPM pipelines.
      Default to MISC_LIBRARY_PATH value in [data-factory:&lt;dataset&gt;:preprocessing] section.
    * PROTOCOLS_DEFINITION_FILE: path to the Protocols definition file defining the protocols used on the scanner.
      Default to PROTOCOLS_DEFINITION_FILE value in [data-factory:&lt;dataset&gt;:preprocessing] section.
    * DCM2NII_PROGRAM: Path to DCM2NII program. Default to PIPELINE_PATH + '/dcm2nii'

"""

import os

from datetime import timedelta
from textwrap import dedent

from airflow import configuration

from airflow_spm.operators import SpmPipelineOperator

from common_steps import Step, default_config


def dicom_to_nifti_pipeline_cfg(dag, upstream_step, preprocessing_section, step_section):
    default_config(preprocessing_section, 'INPUT_CONFIG', '')
    default_config(preprocessing_section, 'PIPELINES_PATH', '.')
    default_config(step_section, 'SPM_FUNCTION', 'DCM2NII_LREN')
    default_config(step_section, 'PIPELINE_PATH', configuration.get(
        preprocessing_section, 'PIPELINES_PATH') + '/Nifti_Conversion_Pipeline')
    default_config(step_section, 'MISC_LIBRARY_PATH', configuration.get(preprocessing_section, 'MISC_LIBRARY_PATH'))
    default_config(step_section, 'PROTOCOLS_DEFINITION_FILE',
                   configuration.get(preprocessing_section, 'PROTOCOLS_DEFINITION_FILE'))
    default_config(step_section, 'DCM2NII_PROGRAM', configuration.get(step_section, 'PIPELINE_PATH') + '/dcm2nii')

    dataset_config = configuration.get(preprocessing_section, 'INPUT_CONFIG')
    pipeline_path = configuration.get(step_section, 'PIPELINE_PATH')
    misc_library_path = configuration.get(step_section, 'MISC_LIBRARY_PATH')
    spm_function = configuration.get(step_section, 'SPM_FUNCTION')
    output_folder = configuration.get(step_section, 'OUTPUT_FOLDER')
    backup_folder = configuration.get(step_section, 'BACKUP_FOLDER')
    protocols_definition_file = configuration.get(step_section, 'PROTOCOLS_DEFINITION_FILE')
    dcm2nii_program = configuration.get(step_section, 'DCM2NII_PROGRAM')

    return dicom_to_nifti_pipeline(dag, upstream_step,
                                   dataset_config=dataset_config,
                                   pipeline_path=pipeline_path,
                                   misc_library_path=misc_library_path,
                                   spm_function=spm_function,
                                   output_folder=output_folder,
                                   backup_folder=backup_folder,
                                   protocols_definition_file=protocols_definition_file,
                                   dcm2nii_program=dcm2nii_program)


def dicom_to_nifti_pipeline(dag, upstream_step,
                            dataset_config='',
                            spm_function='DCM2NII_LREN',
                            pipeline_path=None,
                            misc_library_path=None,
                            output_folder=None,
                            backup_folder=None,
                            protocols_definition_file=None,
                            dcm2nii_program=None):

    def arguments_fn(folder, session_id, **kwargs):
        """
          Prepare the arguments for conversion pipeline from DICOM to Nifti format.
          It converts all files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')

        return [parent_data_folder,
                session_id,
                output_folder,
                backup_folder,
                protocols_definition_file,
                dcm2nii_program]

    dicom_to_nifti_pipeline = SpmPipelineOperator(
        task_id='dicom_to_nifti_pipeline',
        spm_function=spm_function,
        spm_arguments_callable=arguments_fn,
        matlab_paths=[misc_library_path, pipeline_path],
        output_folder_callable=lambda session_id, **kwargs: output_folder + '/' + session_id,
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
        dicom_to_nifti_pipeline.set_upstream(upstream_step.task)

    dicom_to_nifti_pipeline.doc_md = dedent("""\
    # DICOM to Nitfi Pipeline

    SPM function: __%s__

    This function convert the dicom files to Nifti format using the SPM tools and
    [dcm2nii](http://www.mccauslandcenter.sc.edu/mricro/mricron/dcm2nii.html) tool developed by Chris Rorden.

    Nifti files are stored the the following locations:

    * Local folder: __%s__
    * Remote folder: __%s__

    Depends on: __%s__
    """ % (spm_function, output_folder, backup_folder, upstream_step.task_id))

    return Step(dicom_to_nifti_pipeline, 'dicom_to_nifti_pipeline', upstream_step.priority_weight + 10)
