from textwrap import dedent

from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow_pipeline.pipelines import pipeline_trigger

from common_steps import Step


def trigger_preprocessing_pipeline_step(dag, upstream_step, dataset):

    trigger_dag_id = '%s_pre_process_images' % dataset.lower().replace(" ", "_")

    trigger_preprocessing_pipeline = TriggerDagRunOperator(
        task_id="trigger_preprocessing_pipeline",
        trigger_dag_id=trigger_dag_id,
        python_callable=pipeline_trigger(upstream_step.task_id),
        priority_weight=999,
        dag=dag
    )

    trigger_preprocessing_pipeline.set_upstream(upstream_step.task)

    trigger_preprocessing_pipeline.doc_md = dedent("""\
    # Trigger pre-processing pipelines

    Trigger pre-processing pipelines.
    """)

    return Step(trigger_preprocessing_pipeline, trigger_preprocessing_pipeline.task_id,
                upstream_step.priority_weight + 10)
