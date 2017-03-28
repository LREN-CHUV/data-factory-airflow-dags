from textwrap import dedent

from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow_pipeline.pipelines import pipeline_trigger

from common_steps import Step


def trigger_ehr_pipeline_step(dag, upstream_step, dataset):

    trigger_dag_id = '%s_mri_flat_etl_incoming' % dataset.lower().replace(" ", "_")

    trigger_ehr_pipeline = TriggerDagRunOperator(
        task_id="trigger_ehr_pipeline",
        trigger_dag_id=trigger_dag_id,
        python_callable=pipeline_trigger(upstream_step.task_id),
        priority_weight=999,
        dag=dag
    )

    trigger_ehr_pipeline.set_upstream(upstream_step.task)

    trigger_ehr_pipeline.doc_md = dedent("""\
    # Trigger EHR pipelines

    Trigger EHR pipelines.
    """)

    return Step(trigger_ehr_pipeline, trigger_ehr_pipeline.task_id, upstream_step.priority_weight + 10)
