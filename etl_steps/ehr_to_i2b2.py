"""

  ETL steps: EHR to I2B2

  Configuration variables used:

  * EHR_TO_I2B2_CAPTURE_DOCKER_IMAGE
  * EHR_TO_I2B2_CAPTURE_FOLDER

"""

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import DockerPipelineOperator

from common_steps import Step


def ehr_to_i2b2_pipeline_cfg(dag, upstream_step, dataset_section):
    ehr_to_i2b2_capture_docker_image = configuration.get(dataset_section, 'EHR_TO_I2B2_CAPTURE_DOCKER_IMAGE')
    local_folder = configuration.get(dataset_section, 'EHR_TO_I2B2_CAPTURE_FOLDER')

    return ehr_to_i2b2_pipeline(dag, upstream_step, local_folder, ehr_to_i2b2_capture_docker_image)


def ehr_to_i2b2_pipeline(dag, upstream_step, ehr_to_i2b2_capture_folder=None, ehr_to_i2b2_capture_docker_image=''):

    ehr_to_i2b2_pipeline = DockerPipelineOperator(
        task_id='map_ehr_to_i2b2_capture',
        image=ehr_to_i2b2_capture_docker_image,
        force_pull=False,
        api_version="1.18",
        cpus=1,
        mem_limit='256m',
        container_tmp_dir='/tmp/airflow',  # nosec
        container_input_dir='/opt/source',
        container_output_dir='/opt/target',
        output_folder_callable=lambda relative_context_path, **kwargs: "%s/%s" % (
            ehr_to_i2b2_capture_folder, relative_context_path),
        volumes=[
            "/opt/postgresdb.properties:/etc/mipmap/postgresdb.properties"
        ],
        pool='io_intensive',
        parent_task=upstream_step.task,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=24),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dag=dag
    )

    if upstream_step.task:
        ehr_to_i2b2_pipeline.set_upstream(upstream_step.task)

    ehr_to_i2b2_pipeline.doc_md = dedent("""\
    # MipMap ETL: map EHR data to I2B2

    Docker image: __%s__

    * Local folder: __%s__

    Depends on: __%s__
    """ % (ehr_to_i2b2_capture_docker_image, ehr_to_i2b2_capture_folder, upstream_step.task))

    return Step(ehr_to_i2b2_pipeline, 'ehr_to_i2b2_pipeline', upstream_step.priority_weight + 10)
