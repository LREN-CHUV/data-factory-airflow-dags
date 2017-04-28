from datetime import timedelta
from textwrap import dedent

from airflow import configuration

from airflow_scan_folder.operators import ScanFlatFolderPipelineOperator

from airflow_scan_folder.operators.common import extract_context_from_session_path
from airflow_scan_folder.operators.common import session_folder_trigger_dagrun

from common_steps import Step, default_config


def trigger_metadata_pipeline_cfg(dag, upstream_step, dataset, section, step_section):
    default_config(section, 'INPUT_CONFIG', '')
    default_config(step_section, 'DEPTH', '1')

    dataset_config = [flag.strip() for flag in configuration.get(section, 'INPUT_CONFIG').split(',')]
    depth = int(configuration.get(step_section, 'DEPTH'))

    return trigger_metadata_pipeline_step(dag, upstream_step, dataset=dataset, dataset_config=dataset_config,
                                          depth=depth)


def trigger_metadata_pipeline_step(dag, upstream_step, dataset, dataset_config, depth=1):

    trigger_dag_id = '%s_import_metadata' % dataset.lower().replace(" ", "_")

    trigger_metadata_pipeline = ScanFlatFolderPipelineOperator(
        task_id='trigger_metadata_pipeline',
        trigger_dag_id=trigger_dag_id,
        trigger_dag_run_callable=session_folder_trigger_dagrun,
        extract_context_callable=extract_context_from_session_path,
        depth=depth,
        dataset_config=dataset_config,
        parent_task=upstream_step.task_id,
        execution_timeout=timedelta(minutes=30),
        priority_weight=999,
        dag=dag,
        organised_folder=False)

    trigger_metadata_pipeline.set_upstream(upstream_step.task)

    trigger_metadata_pipeline.doc_md = dedent("""\
    # Trigger metadata pipelines

    Trigger metadata pipelines.
    """)

    return Step(trigger_metadata_pipeline, trigger_metadata_pipeline.task_id,
                upstream_step.priority_weight + 10)
