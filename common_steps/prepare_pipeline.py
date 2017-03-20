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

    prepare_pipeline = PreparePipelineOperator(
        task_id='prepare_pipeline',
        include_spm_facts=include_spm_facts,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(minutes=10),
        dag=dag
    )

    prepare_pipeline.set_upstream(upstream_step.task)

    prepare_pipeline.doc_md = dedent("""\
    # Prepare pipeline

    Add information required by the Pipeline operators.
    """)

    return Step(prepare_pipeline, 'prepare_pipeline', upstream_step.priority_weight + 10)
