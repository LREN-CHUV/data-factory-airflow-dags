"""

  Common step step: prepare pipeline

  Configuration variables used:

  None

"""

from datetime import timedelta
from textwrap import dedent

from airflow_pipeline.operators import PreparePipelineOperator


def prepare_pipeline(dag, upstream, upstream_id, priority_weight, include_spm_facts=True):

    prepare_pipeline = PreparePipelineOperator(
        task_id='prepare_pipeline',
        include_spm_facts=include_spm_facts,
        priority_weight=priority_weight,
        execution_timeout=timedelta(minutes=10),
        dag=dag
    )

    prepare_pipeline.set_upstream(upstream)

    prepare_pipeline.doc_md = dedent("""\
    # Prepare pipeline

    Add information required by the Pipeline operators.
    """)

    upstream = prepare_pipeline
    upstream_id = 'prepare_pipeline'
    priority_weight += 10

    return (upstream, upstream_id, priority_weight)
