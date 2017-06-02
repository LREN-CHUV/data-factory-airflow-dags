from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_scan_folder.operators import ScanFlatFolderPipelineOperator
from airflow_scan_folder.operators.common import default_extract_context
from airflow_scan_folder.operators.common import default_trigger_dagrun

from common_steps import Step, default_config


def trigger_metadata_pipeline_cfg(dag, upstream_step, dataset, step_section):
    default_config(step_section, 'DEPTH', '0')
    depth = int(configuration.get(step_section, 'DEPTH'))
    return trigger_metadata_pipeline_step(dag, upstream_step, dataset=dataset, depth=depth)


def trigger_metadata_pipeline_step(dag, upstream_step, dataset, depth=0):

    trigger_dag_id = '%s_metadata_import' % dataset.lower().replace(" ", "_")

    trigger_metadata_pipeline = ScanFlatFolderPipelineOperator(
        task_id='trigger_metadata_pipeline',
        trigger_dag_id=trigger_dag_id,
        trigger_dag_run_callable=default_trigger_dagrun,
        extract_context_callable=default_extract_context,
        source_folder_param='metadata_folder',
        depth=depth,
        parent_task=upstream_step.task_id,
        execution_timeout=timedelta(minutes=30),
        priority_weight=999,
        dag=dag,
        organised_folder=False
    )

    trigger_metadata_pipeline.set_upstream(upstream_step.task)

    trigger_metadata_pipeline.doc_md = dedent("""\
    # Trigger metadata pipelines

    Trigger metadata pipelines.
    """)

    return Step(trigger_metadata_pipeline, trigger_metadata_pipeline.task_id, upstream_step.priority_weight + 10)
