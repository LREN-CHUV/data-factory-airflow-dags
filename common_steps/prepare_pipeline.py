"""

Common step step: prepare pipeline

Configuration variables used:

None

"""

from datetime import timedelta
from textwrap import dedent

from airflow_pipeline.operators import PreparePipelineOperator

from common_steps import Step


def prepare_pipeline(dag, upstream_step, include_spm_facts=True):

    prepare_pipeline_op = PreparePipelineOperator(
        task_id='prepare_pipeline',
        include_spm_facts=include_spm_facts,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(minutes=10),
        dag=dag
    )

    if upstream_step.task:
        prepare_pipeline_op.set_upstream(upstream_step.task)

    prepare_pipeline_op.doc_md = dedent("""\
    # Prepare pipeline

    Add information used by the other pipeline operators downstream.

    This includes dataset and folder to process, information to help tracking provenance.
    """)

    return Step(prepare_pipeline_op, prepare_pipeline_op.task_id, upstream_step.priority_weight + 10)
