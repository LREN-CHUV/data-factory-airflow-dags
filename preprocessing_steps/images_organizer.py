"""

  Pre processing step: images organizer

  Configuration variables used:

  * DATASET_CONFIG
  * IMAGES_ORGANIZER_DATASET_TYPE
  * IMAGES_ORGANIZER_DATA_STRUCTURE
  * IMAGES_ORGANIZER_LOCAL_FOLDER
  * IMAGES_ORGANIZER_DOCKER_IMAGE
  * <input_folder_config_key>

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import DockerPipelineOperator

from common_steps import Step


def images_organizer_cfg(dag, upstream_step, dataset_section, dataset,
                         input_folder_config_key):
    dataset_config = configuration.get(dataset_section, 'DATASET_CONFIG')
    dataset_type = configuration.get(dataset_section, 'IMAGES_ORGANIZER_DATASET_TYPE')
    data_structure = configuration.get(dataset_section, 'IMAGES_ORGANIZER_DATA_STRUCTURE')
    local_folder = configuration.get(dataset_section, 'IMAGES_ORGANIZER_LOCAL_FOLDER')
    docker_image = configuration.get(dataset_section, 'IMAGES_ORGANIZER_DOCKER_IMAGE')
    input_folder = configuration.get(dataset_section, input_folder_config_key)

    return images_organizer(dag, upstream_step, dataset, dataset_config,
                            dataset_type=dataset_type,
                            data_structure=data_structure,
                            input_folder=input_folder,
                            local_folder=local_folder,
                            docker_image=docker_image)


def images_organizer(dag, upstream_step, dataset, dataset_config,
                     dataset_type, data_structure, input_folder,
                     local_folder, docker_image='hbpmip/hierarchizer:latest'):

    dataset_param = "--dataset " + dataset
    type_of_images_param = "--type " + dataset_type
    structure_param = "--attributes " + str(data_structure.split(':'))

    images_organizer_pipeline = DockerPipelineOperator(
        task_id='images_organizer_pipeline',
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
        command=[dataset_param, type_of_images_param, structure_param],
        volumes=[input_folder + ':/input_folder:ro',
                 local_folder + ':/output_folder']
    )

    if upstream_step.task:
        images_organizer_pipeline.set_upstream(upstream_step.task)

    images_organizer_pipeline.doc_md = dedent("""\
        # Images organizer pipeline

        Reorganise DICOM/NIFTI files to fit the structure expected by the following pipelines.

        Reorganised DICOM/NIFTI files are stored the the following locations:

        * Local folder: __%s__

        Depends on: __%s__
        """ % (local_folder, upstream_step.task_id))

    return Step(images_organizer_pipeline, 'images_organizer_pipeline', upstream_step.priority_weight + 10)
