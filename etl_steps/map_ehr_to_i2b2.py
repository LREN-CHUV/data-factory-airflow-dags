"""

ETL steps: Map EHR data to I2B2.

Configuration variables used:

* :ehr:map_ehr_to_i2b2 section:
    * DOCKER_IMAGE

"""

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import DockerPipelineOperator

from common_steps import Step


def map_ehr_to_i2b2_pipeline_cfg(dag, upstream_step, etl_section, step_section):
    docker_image = configuration.get(step_section, 'DOCKER_IMAGE')

    return map_ehr_to_i2b2_pipeline_step(dag, upstream_step, docker_image)


def map_ehr_to_i2b2_pipeline_step(dag, upstream_step, docker_image=''):

    map_ehr_to_i2b2_pipeline = DockerPipelineOperator(
        task_id='map_ehr_to_i2b2_pipeline',
        image=docker_image,
        force_pull=False,
        api_version="1.18",
        cpus=1,
        mem_limit='256m',
        container_tmp_dir='/tmp/airflow',  # nosec
        container_input_dir='/opt/source',
        container_output_dir='/opt/target',
        output_folder_callable=None,
        volumes=[
            "/opt/postgresdb.properties:/etc/mipmap/postgresdb.properties"
        ],
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(minutes=60),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dag=dag
    )

    if upstream_step.task:
        map_ehr_to_i2b2_pipeline.set_upstream(upstream_step.task)

    map_ehr_to_i2b2_pipeline.doc_md = dedent("""\
    # MipMap ETL: map EHR data to I2B2

    Docker image: __%s__

    Depends on: __%s__
    """ % (docker_image, upstream_step.task_id))

    return Step(map_ehr_to_i2b2_pipeline, map_ehr_to_i2b2_pipeline.task_id, upstream_step.priority_weight + 10)
