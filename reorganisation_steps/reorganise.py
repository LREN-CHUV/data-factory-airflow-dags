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
    default_config(step_section, "ALLOWED_FIELD_VALUES", '')

    dataset_config = [flag.strip() for flag in configuration.get(reorganisation_section, 'INPUT_CONFIG').split(',')]
    output_folder = configuration.get(step_section, 'OUTPUT_FOLDER')
    meta_output_folder = configuration.get(step_section, 'META_OUTPUT_FOLDER')
    output_folder_structure = configuration.get(step_section, 'OUTPUT_FOLDER_STRUCTURE')
    allowed_field_values = configuration.get(step_section, 'ALLOWED_FIELD_VALUES')
    docker_image = configuration.get(step_section, 'DOCKER_IMAGE')
    docker_input_dir = configuration.get(step_section, 'DOCKER_INPUT_DIR')
    docker_output_dir = configuration.get(step_section, 'DOCKER_OUTPUT_DIR')
    docker_user = configuration.get(step_section, 'DOCKER_USER')

    m = re.search('.*:reorganisation:(.*)_reorganise', step_section)
    dataset_type = m.group(1).upper()

    return reorganise_pipeline_step(dag, upstream_step, dataset_config,
                                    dataset_type=dataset_type,
                                    output_folder_structure=output_folder_structure,
                                    output_folder=output_folder,
                                    meta_output_folder=meta_output_folder,
                                    allowed_field_values=allowed_field_values,
                                    docker_image=docker_image,
                                    docker_input_dir=docker_input_dir,
                                    docker_output_dir=docker_output_dir,
                                    docker_user=docker_user)


def reorganise_pipeline_step(
        dag, upstream_step, dataset_config, dataset_type, output_folder_structure, output_folder, meta_output_folder,
        allowed_field_values=None,
        docker_image='hbpmip/hierarchizer:latest',
        docker_input_dir='/input_folder',
        docker_output_dir='/output_folder',
        docker_user='root'):

    incoming_dataset_param = "{{ dag_run.conf['dataset'] }}"
    type_of_images_param = "--type " + dataset_type
    structure_param = "--output_folder_organisation " + output_folder_structure
    allowed_field_values = ("--allowed_field_values " + allowed_field_values) if allowed_field_values else ""
    command = "%s %s %s %s" % (incoming_dataset_param, type_of_images_param, structure_param, allowed_field_values)

    reorganise_pipeline = DockerPipelineOperator(
        task_id='reorganise_%s_pipeline' % dataset_type.lower(),
        output_folder_callable=lambda **kwargs: output_folder,
        metadata_folder_callable=lambda **kwargs: meta_output_folder,
        cleanup_output_folder=False,
        pool='io_intensive',
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=24),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        organised_folder=True,
        dag=dag,
        image=docker_image,
        command=command,
        container_input_dir=docker_input_dir,
        container_output_dir=docker_output_dir,
        user=docker_user,
        volumes=[meta_output_folder + ":/meta_output_folder"]
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
        """ % (docker_image, output_folder, upstream_step.task_id))

    return Step(reorganise_pipeline, reorganise_pipeline.task_id, upstream_step.priority_weight + 10)
