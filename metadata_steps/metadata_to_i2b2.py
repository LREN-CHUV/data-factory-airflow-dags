"""

Metadata step: metadata to I2B2.

Import imaging metadata from various files to the I2B2 database.

Configuration variables used:

* data-factory section
  * I2B2_SQL_ALCHEMY_CONN

"""

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import PythonPipelineOperator

from common_steps import Step

from i2b2_import import meta_files_import


def metadata_to_i2b2_pipeline_cfg(dag, upstream_step, data_factory_section):
    i2b2_conn = configuration.get(data_factory_section, 'I2B2_SQL_ALCHEMY_CONN')

    return metadata_to_i2b2_pipeline_step(dag, upstream_step, i2b2_conn)


def metadata_to_i2b2_pipeline_step(dag, upstream_step, i2b2_conn):

    def metadata_to_i2b2_fn(dataset, extra_info, **kwargs):
        meta_files_import.folder2db(extra_info['meta_output_folder'], i2b2_conn, dataset)
        return "ok"

    metadata_to_i2b2_pipeline = PythonPipelineOperator(
        task_id='metadata_to_i2b2_pipeline',
        python_callable=metadata_to_i2b2_fn,
        pool='io_intensive',
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=6),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dag=dag
    )

    if upstream_step.task:
        metadata_to_i2b2_pipeline.set_upstream(upstream_step.task)

    metadata_to_i2b2_pipeline.doc_md = dedent("""\
        # Metadata to I2B2 pipeline

        Import imaging metadata from various files to the I2B2 database.

        Depends on: __%s__
        """ % upstream_step.task_id)

    return Step(metadata_to_i2b2_pipeline, metadata_to_i2b2_pipeline.task_id, upstream_step.priority_weight + 10)
