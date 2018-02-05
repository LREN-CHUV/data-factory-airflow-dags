"""

Pre processing step: Neuromorphometric Atlas.

Computes an individual Atlas based on the NeuroMorphometrics Atlas. This is based on the NeuroMorphometrics Toolbox.
This delivers three files:

1) Atlas File (*.nii);
2) Volumes of the Morphometric Atlas structures (*.txt);
3) Excel File (.xls) or *.csv containing the volume, and globals plus Multiparametric Maps (R2*, R1, MT, PD) for
   each structure defined in the Subject Atlas.

In case of anatomical images different from Multiparametric Maps the outputs will be only the structure volumes.

Configuration variables used:

* :preprocessing section
    * INPUT_CONFIG: List of flags defining how incoming imaging data are organised.
    * PIPELINES_PATH: Path to the root folder containing the Matlab scripts for the pipelines.
* :preprocessing:mpm_maps section
    * PIPELINE_PATH: path to the folder containing the SPM script for this pipeline.
      Default to PIPELINES_PATH + '/MPMs_Pipeline'
* :preprocessing:neuro_morphometric_atlas section
    * OUTPUT_FOLDER: destination folder for the Atlas File, the volumes of the Morphometric Atlas structures (*.txt),
      the csv file containing the volume, and globals plus Multiparametric Maps (R2*, R1, MT, PD) for each structure
      defined in the Subject Atlas.
    * BACKUP_FOLDER: backup folder for the Atlas File, the volumes of the Morphometric Atlas structures (*.txt),
      the csv file containing the volume, and globals plus Multiparametric Maps (R2*, R1, MT, PD) for each structure
      defined in the Subject Atlas.
    * SPM_FUNCTION: SPM function called. Default to 'NeuroMorphometric_pipeline'
    * PIPELINE_PATH: path to the folder containing the SPM script for this pipeline.
      Default to PIPELINES_PATH + '/NeuroMorphometric_Pipeline/NeuroMorphometric_tbx/label'
    * MISC_LIBRARY_PATH: path to the Misc&Libraries folder for SPM pipelines.
      Default to MISC_LIBRARY_PATH value in [data-factory:&lt;dataset&gt;:preprocessing] section.
    * PROTOCOLS_DEFINITION_FILE: path to the Protocols definition file defining the protocols used on the scanner.
      Default to PROTOCOLS_DEFINITION_FILE value in [data-factory:&lt;dataset&gt;:preprocessing] section.
    * TPM_TEMPLATE: Path to the the template used for segmentation step in case the image is not segmented.
      Default to SPM_DIR + '/tpm/nwTPM_sl3.nii'
    * MATLAB_USER: Run Matlab as specified user

"""

import os

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_spm.operators import SpmPipelineOperator
from common_steps import Step, default_config


def neuro_morphometric_atlas_pipeline_cfg(dag, upstream_step, preprocessing_section, step_section):
    default_config(preprocessing_section, 'INPUT_CONFIG', '')
    default_config(preprocessing_section, 'PIPELINES_PATH', '.')
    default_config(step_section, 'SPM_FUNCTION', 'NeuroMorphometric_pipeline')
    default_config(step_section, 'PIPELINE_PATH', configuration.get(
        preprocessing_section, 'PIPELINES_PATH') + '/NeuroMorphometric_Pipeline/NeuroMorphometric_tbx/label',
        fill_empty=True)
    default_config(step_section, 'MISC_LIBRARY_PATH', configuration.get(
        preprocessing_section, 'MISC_LIBRARY_PATH'), fill_empty=True)
    default_config(step_section, 'PROTOCOLS_DEFINITION_FILE',
                   configuration.get(preprocessing_section, 'PROTOCOLS_DEFINITION_FILE'), fill_empty=True)
    default_config(step_section, 'TPM_TEMPLATE',
                   configuration.get('spm', 'SPM_DIR') + '/tpm/TPM.nii')
    default_config(step_section, 'BACKUP_FOLDER', '')
    mpm_maps_section = preprocessing_section + ':mpm_maps'
    try:
        default_config(mpm_maps_section, 'PIPELINE_PATH', configuration.get(
            preprocessing_section, 'PIPELINES_PATH') + '/MPMs_Pipeline')
    except Exception:
        # No need to do anything
        pass

    dataset_config = [flag.strip() for flag in configuration.get(preprocessing_section, 'INPUT_CONFIG').split(',')]
    pipeline_path = configuration.get(step_section, 'PIPELINE_PATH')
    misc_library_path = configuration.get(step_section, 'MISC_LIBRARY_PATH')
    spm_function = configuration.get(step_section, 'SPM_FUNCTION')
    output_folder = configuration.get(step_section, 'OUTPUT_FOLDER')
    backup_folder = configuration.get(step_section, 'BACKUP_FOLDER')
    protocols_definition_file = configuration.get(step_section, 'PROTOCOLS_DEFINITION_FILE')
    tpm_template = configuration.get(step_section, 'TPM_TEMPLATE')
    try:
        mpm_maps_pipeline_path = configuration.get(mpm_maps_section, 'PIPELINE_PATH')
    except Exception:
        mpm_maps_pipeline_path = configuration.get(preprocessing_section, 'PIPELINES_PATH') + '/MPMs_Pipeline'
    user = configuration.get(step_section, 'MATLAB_USER')

    # check that file exists if absolute path
    if len(tpm_template) > 0 and tpm_template[0] is '/':
        if not os.path.isfile(tpm_template):
            raise OSError("TPM template file %s does not exist" % tpm_template)

    return neuro_morphometric_atlas_pipeline_step(dag, upstream_step,
                                                  dataset_config=dataset_config,
                                                  pipeline_path=pipeline_path,
                                                  misc_library_path=misc_library_path,
                                                  spm_function=spm_function,
                                                  output_folder=output_folder,
                                                  backup_folder=backup_folder,
                                                  protocols_definition_file=protocols_definition_file,
                                                  tpm_template=tpm_template,
                                                  mpm_maps_pipeline_path=mpm_maps_pipeline_path,
                                                  user=user)


def neuro_morphometric_atlas_pipeline_step(dag, upstream_step,
                                           dataset_config=None,
                                           pipeline_path=None,
                                           misc_library_path=None,
                                           spm_function='NeuroMorphometric_pipeline',
                                           output_folder=None,
                                           backup_folder='',
                                           protocols_definition_file=None,
                                           tpm_template='nwTPM_sl3.nii',
                                           mpm_maps_pipeline_path=None,
                                           user='airflow'):

    def arguments_fn(folder, session_id, **kwargs):
        """Prepare the arguments for the pipeline that selects T1 files from DICOM.

        It selects all T1 files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')
        table_format = 'csv'

        return [session_id,
                parent_data_folder,
                output_folder,
                backup_folder,
                protocols_definition_file,
                table_format,
                tpm_template]

    neuro_morphometric_atlas_pipeline = SpmPipelineOperator(
        task_id='neuro_morphometric_atlas_pipeline',
        spm_function=spm_function,
        spm_arguments_callable=arguments_fn,
        matlab_paths=[misc_library_path,
                      pipeline_path,
                      mpm_maps_pipeline_path],
        output_folder_callable=lambda session_id, **kwargs: (output_folder + '/' + session_id),
        pool='image_preprocessing',
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=24),
        on_skip_trigger_dag_id='mri_notify_skipped_processing',
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag,
        organised_folder=True,
        run_as_user=user
    )
    neuro_morphometric_atlas_pipeline.set_upstream(upstream_step.task)

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

            * Target folder: __%s__
            * Remote folder: __%s__

            Depends on: __%s__
            """ % (spm_function, output_folder, backup_folder, upstream_step.task_id))

    return Step(neuro_morphometric_atlas_pipeline, neuro_morphometric_atlas_pipeline.task_id,
                upstream_step.priority_weight + 10)
