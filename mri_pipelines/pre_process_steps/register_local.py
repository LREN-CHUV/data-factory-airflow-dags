"""

  Pre processing step: register local files

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import BashPipelineOperator


def register_local_cfg(upstream, upstream_id, priority_weight, dataset_section):
    dataset_config = configuration.get(dataset_section, 'DATASET_CONFIG')

    return register_local(upstream, upstream_id, priority_weight, dataset_config)


def register_local(upstream, upstream_id, priority_weight, dataset_config):

    # Register local data into the Data catalog/provenance tables

    register_local_cmd = ";"

    register_local = BashPipelineOperator(
        task_id='register_local',
        bash_command=register_local_cmd,
        params={},
        parent_task=upstream_id,
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=3),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag
    )
    register_local.set_upstream(upstream)

    register_local.doc_md = dedent("""\
        # Register incoming files for provenance

        This step does nothing except register the files in the input folder for provenance.
        """)

    upstream = register_local
    upstream_id = 'register_local'
    priority_weight += 10

    return (upstream, upstream_id, priority_weight)
