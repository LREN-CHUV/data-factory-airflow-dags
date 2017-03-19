"""

  Pre processing step: Neuromorphometric Atlas

  Configuration variables used:

  * DATASET_CONFIG
  * PIPELINES_PATH
  * NEURO_MORPHOMETRIC_ATLAS_SPM_FUNCTION
  * NEURO_MORPHOMETRIC_ATLAS_OUTPUT_FOLDER
  * NEURO_MORPHOMETRIC_ATLAS_SERVER_FOLDER
  * NEURO_MORPHOMETRIC_ATLAS_TPM_TEMPLATE

"""

import os

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_spm.operators import SpmPipelineOperator
from common_steps import Step, default_config


def neuro_morphometric_atlas_pipeline_cfg(dag, upstream_step, dataset_section):
    default_config(dataset_section, 'DATASET_CONFIG', '')
    default_config(dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_SPM_FUNCTION',
                   'NeuroMorphometric_pipeline')
    default_config(dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_TPM_TEMPLATE',
                   configuration.get('spm', 'SPM_DIR') + '/tpm/nwTPM_sl3.nii')

    dataset_config = configuration.get(dataset_section, 'DATASET_CONFIG')
    neuro_morphometric_atlas_pipeline_path = configuration.get(
        dataset_section,
        'PIPELINES_PATH') + '/NeuroMorphometric_Pipeline/NeuroMorphometric_tbx/label'
    mpm_maps_pipeline_path = configuration.get(dataset_section, 'PIPELINES_PATH') + '/MPMs_Pipeline'
    misc_library_path = configuration.get(dataset_section, 'PIPELINES_PATH') + '/../Miscellaneous&Others'
    protocols_file = configuration.get(dataset_section, 'PROTOCOLS_FILE')
    spm_function = configuration.get(dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_SPM_FUNCTION')
    local_folder = configuration.get(dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_OUTPUT_FOLDER')
    server_folder = configuration.get(dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_SERVER_FOLDER')
    tpm_template = configuration.get(dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_TPM_TEMPLATE')

    # check that file exists if absolute path
    if len(tpm_template) > 0 and tpm_template[0] is '/':
        if not os.path.isfile(tpm_template):
            raise OSError("TPM template file %s does not exist" % tpm_template)

    return neuro_morphometric_atlas_pipeline(dag, upstream_step,
                                             dataset_config=dataset_config,
                                             neuro_morpho_atlas_pipeline_path=neuro_morphometric_atlas_pipeline_path,
                                             mpm_maps_pipeline_path=mpm_maps_pipeline_path,
                                             misc_library_path=misc_library_path,
                                             spm_function=spm_function,
                                             local_folder=local_folder,
                                             server_folder=server_folder,
                                             tpm_template=tpm_template,
                                             protocols_file=protocols_file)


def neuro_morphometric_atlas_pipeline(dag, upstream_step,
                                      dataset_config=None,
                                      neuro_morpho_atlas_pipeline_path=None,
                                      mpm_maps_pipeline_path=None,
                                      misc_library_path=None,
                                      spm_function='NeuroMorphometric_pipeline',
                                      local_folder=None,
                                      server_folder='',
                                      tpm_template='nwTPM_sl3.nii',
                                      protocols_file=None):

    def arguments_fn(folder, session_id, **kwargs):
        """
          Prepare the arguments for the pipeline that selects T1 files from DICOM.
          It selects all T1 files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')
        table_format = 'csv'

        return [session_id,
                parent_data_folder,
                local_folder,
                server_folder,
                protocols_file,
                table_format,
                tpm_template]

    neuro_morphometric_atlas_pipeline = SpmPipelineOperator(
        task_id='neuro_morphometric_atlas_pipeline',
        spm_function=spm_function,
        spm_arguments_callable=arguments_fn,
        matlab_paths=[misc_library_path,
                      neuro_morpho_atlas_pipeline_path,
                      mpm_maps_pipeline_path],
        output_folder_callable=lambda session_id, **kwargs: (local_folder + '/' + session_id),
        pool='image_preprocessing',
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=24),
        on_skip_trigger_dag_id='mri_notify_skipped_processing',
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag
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

            * Local folder: %s
            * Remote folder: %s

            Depends on: __%s__
            """ % (spm_function, local_folder, server_folder, upstream_step.task_id))

    return Step(neuro_morphometric_atlas_pipeline, 'neuro_morphometric_atlas_pipeline',
                upstream_step.priority_weight + 10)
