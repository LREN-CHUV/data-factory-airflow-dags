"""

Pre processing step: images organiser.

Reorganises DICOM files in a scan folder for the following pipelines.

Configuration variables used:

* :reorganisation section
    * INPUT_CONFIG: List of flags defining how incoming imaging data are organised.
* :reorganisation:dicom_organiser or :reorganisation:nifti_organiser section
    * OUTPUT_FOLDER: destination folder for the organised images
    * OUTPUT_FOLDER_STRUCTURE: folder hierarchy (e.g. 'PatientID:AcquisitionDate:SeriesDescription:SeriesDate')
    * DOCKER_IMAGE: Docker image of the hierarchizer program
    * DOCKER_INPUT_DIR: Input directory inside the Docker container. Default to '/input_folder'
    * DOCKER_OUTPUT_DIR: Output directory inside the Docker container. Default to '/output_folder'

"""


import re

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import DockerPipelineOperator

from common_steps import Step, default_config


def reorganise_cfg(dag, upstream_step, reorganisation_section, step_section):
    default_config(reorganisation_section, 'INPUT_CONFIG', '')
    default_config(step_section, "DOCKER_INPUT_DIR", "/input_folder")
    default_config(step_section, "DOCKER_OUTPUT_DIR", "/output_folder")

    dataset_config = configuration.get(reorganisation_section, 'INPUT_CONFIG')
    local_folder = configuration.get(step_section, 'OUTPUT_FOLDER')
    output_folder_structure = configuration.get(step_section, 'OUTPUT_FOLDER_STRUCTURE')
    docker_image = configuration.get(step_section, 'DOCKER_IMAGE')
    docker_input_dir = configuration.get(step_section, 'DOCKER_INPUT_DIR')
    docker_output_dir = configuration.get(step_section, 'DOCKER_OUTPUT_DIR')

    m = re.search('.*:reorganisation:(.*)_reorganise', step_section)
    dataset_type = m.group(1).upper()

    return reorganise(dag, upstream_step, dataset_config,
                      dataset_type=dataset_type,
                      output_folder_structure=output_folder_structure,
                      local_folder=local_folder,
                      docker_image=docker_image,
                      docker_input_dir=docker_input_dir,
                      docker_output_dir=docker_output_dir)


def reorganise(dag, upstream_step, dataset_config,
               dataset_type, output_folder_structure,
               local_folder,
               docker_image='hbpmip/hierarchizer:latest',
               docker_input_dir='/input_folder',
               docker_output_dir='/output_folder'):

    type_of_images_param = "--type " + dataset_type
    structure_param = "--output_folder_organisation " + output_folder_structure
    incoming_dataset_param = "--incoming_dataset {{ dag_run.conf['dataset'] }}"
    command = "%s %s %s" % (type_of_images_param, structure_param, incoming_dataset_param)

    reorganise_pipeline = DockerPipelineOperator(
        task_id='reorganise_pipeline',
        output_folder_callable=lambda session_id, **kwargs: local_folder + '/' + session_id,
        pool='io_intensive',
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=24),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag,
        image=docker_image,
        command=command,
        container_input_dir=docker_input_dir,
        container_output_dir=docker_output_dir
    )

    if upstream_step.task:
        reorganise_pipeline.set_upstream(upstream_step.task)

    reorganise_pipeline.doc_md = dedent("""\
        # Reorganise pipeline

        Reorganise DICOM/NIFTI files to fit the structure expected by the following pipelines.

        Docker image: __%s__

        Reorganised files are stored in the following location:

        * Target folder: __%s__

        Depends on: __%s__
        """ % (docker_image, local_folder, upstream_step.task_id))

    return Step(reorganise_pipeline, 'reorganise_pipeline', upstream_step.priority_weight + 10)
