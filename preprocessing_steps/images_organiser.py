"""

Pre processing step: images organiser.

Reorganises DICOM files in a scan folder for the following pipelines.

Configuration variables used:

* :preprocessing section
    * INPUT_CONFIG: List of flags defining how incoming imaging data are organised.
* :preprocessing:dicom_organiser or :preprocessing:nifti_organiser section
    * OUTPUT_FOLDER: destination folder for the organised images
    * DATA_STRUCTURE: folder hierarchy (e.g. 'PatientID:AcquisitionDate:SeriesDescription:SeriesDate')
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


def images_organiser_cfg(dag, upstream_step, preprocessing_section, step_section):
    default_config(preprocessing_section, 'INPUT_CONFIG', '')
    default_config(step_section, "DOCKER_INPUT_DIR", "/input_folder")
    default_config(step_section, "DOCKER_OUTPUT_DIR", "/output_folder")

    dataset_config = configuration.get(preprocessing_section, 'INPUT_CONFIG')
    local_folder = configuration.get(step_section, 'OUTPUT_FOLDER')
    data_structure = configuration.get(step_section, 'DATA_STRUCTURE')
    docker_image = configuration.get(step_section, 'DOCKER_IMAGE')
    docker_input_dir = configuration.get(step_section, 'DOCKER_INPUT_DIR')
    docker_output_dir = configuration.get(step_section, 'DOCKER_OUTPUT_DIR')

    m = re.search('.*:preprocessing:(.*)_organiser', step_section)
    dataset_type = m.group(1)

    return images_organiser(dag, upstream_step, dataset_config,
                            dataset_type=dataset_type,
                            data_structure=data_structure,
                            local_folder=local_folder,
                            docker_image=docker_image,
                            docker_input_dir=docker_input_dir,
                            docker_output_dir=docker_output_dir)


def images_organiser(dag, upstream_step, dataset_config,
                     dataset_type, data_structure,
                     local_folder,
                     docker_image='hbpmip/hierarchizer:latest',
                     docker_input_dir='/input_folder',
                     docker_output_dir='/output_folder'):

    type_of_images_param = "--type " + dataset_type
    structure_param = "--output_folder_organisation " + str(data_structure.split(':'))
    incoming_dataset_param = "--incoming_dataset {{ dag_run.conf['dataset'] }}"
    command = "%s %s %s" % (type_of_images_param, structure_param, incoming_dataset_param)

    images_organiser_pipeline = DockerPipelineOperator(
        task_id='images_organiser_pipeline',
        output_folder_callable=lambda session_id, **kwargs: local_folder + '/' + session_id,
        pool='io_intensive',
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=24),
        on_skip_trigger_dag_id='mri_notify_skipped_processing',
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag,
        image=docker_image,
        command=command,
        container_input_dir=docker_input_dir,
        container_output_dir=docker_output_dir
    )

    if upstream_step.task:
        images_organiser_pipeline.set_upstream(upstream_step.task)

    images_organiser_pipeline.doc_md = dedent("""\
        # Images organiser pipeline

        Reorganise DICOM/NIFTI files to fit the structure expected by the following pipelines.

        Reorganised DICOM/NIFTI files are stored the the following locations:

        * Local folder: __%s__

        Depends on: __%s__
        """ % (local_folder, upstream_step.task_id))

    return Step(images_organiser_pipeline, 'images_organiser_pipeline', upstream_step.priority_weight + 10)
