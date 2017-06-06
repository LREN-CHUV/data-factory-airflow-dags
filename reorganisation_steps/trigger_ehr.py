from textwrap import dedent

from airflow import configuration

from airflow_scan_folder.operators import ScanFlatFolderPipelineOperator

from common_steps import Step, default_config


def trigger_ehr_pipeline_cfg(dag, upstream_step, dataset, section, step_section):
    default_config(section, 'INPUT_CONFIG', '')
    default_config(step_section, 'DEPTH', '0')

    dataset_config = [flag.strip() for flag in configuration.get(section, 'INPUT_CONFIG').split(',')]
    depth = int(configuration.get(step_section, 'DEPTH'))

    return trigger_ehr_pipeline_step(dag, upstream_step, dataset=dataset,
                                     dataset_config=dataset_config,
                                     depth=depth)


def trigger_ehr_pipeline_step(dag, upstream_step, dataset, dataset_config, depth=1):

    trigger_dag_id = '%s_mri_flat_ehr_incoming' % dataset.lower().replace(" ", "_")

    trigger_ehr_pipeline = ScanFlatFolderPipelineOperator(
        task_id="trigger_ehr_pipeline",
        trigger_dag_id=trigger_dag_id,
        depth=depth,
        dataset_config=dataset_config,
        parent_task=upstream_step.task_id,
        priority_weight=999,
        dag=dag,
        organised_folder=False
    )

    trigger_ehr_pipeline.set_upstream(upstream_step.task)

    trigger_ehr_pipeline.doc_md = dedent("""\
    # Trigger EHR pipelines

    Trigger EHR pipelines.
    """)

    return Step(trigger_ehr_pipeline, trigger_ehr_pipeline.task_id, upstream_step.priority_weight + 10)
