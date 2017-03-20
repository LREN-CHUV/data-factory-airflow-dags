"""

Pre processing step: notify success

Configuration variables used:

None

"""


from textwrap import dedent

from airflow.operators import TriggerDagRunOperator
from airflow_pipeline.pipelines import pipeline_trigger

from common_steps import Step


def notify_success(dag, upstream_step):

    notify_success = TriggerDagRunOperator(
        task_id='notify_success',
        trigger_dag_id='mri_notify_successful_processing',
        python_callable=pipeline_trigger('extract_nifti_atlas_info'),
        priority_weight=999,
        dag=dag
    )

    notify_success.set_upstream(upstream_step.task)

    notify_success.doc_md = dedent("""\
    # Notify successful processing

    Notify successful processing of this MRI scan session.
    """)

    return Step(notify_success, 'notify_success', upstream_step.priority_weight + 10)
