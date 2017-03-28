"""

Pre processing step: register local files

Configuration variables used:

* :preprocessing section
    * INPUT_CONFIG: List of flags defining how incoming imaging data are organised.

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import BashPipelineOperator

from common_steps import Step, default_config


def register_local_cfg(dag, upstream_step, preprocessing_section):
    default_config(preprocessing_section, 'INPUT_CONFIG', '')

    dataset_config = configuration.get(preprocessing_section, 'INPUT_CONFIG')

    return register_local_step(dag, upstream_step, dataset_config)


def register_local_step(dag, upstream_step, dataset_config):

    # Register local data into the Data catalog/provenance tables

    register_local_cmd = "echo 'Register files in folder $AIRFLOW_OUTPUT_DIR'"

    register_local = BashPipelineOperator(
        task_id='register_local',
        bash_command=register_local_cmd,
        params={},
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=3),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag
    )

    if upstream_step.task:
        register_local.set_upstream(upstream_step.task)

    register_local.doc_md = dedent("""\
        # Register incoming files for provenance

        This step does nothing except register the files present in the input folder to track provenance.
        """)

    return Step(register_local, register_local.task_id, upstream_step.priority_weight + 10)
