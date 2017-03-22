"""

Pre processing step: cleanup local folder.

Cleanup the local folder created during copy_to_local step.

Configuration variables used:

* :preprocessing:copy_to_local section
    * OUTPUT_FOLDER: destination folder for the local copy

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow.operators import BashOperator

from common_steps import Step


def cleanup_local_cfg(dag, upstream_step, preprocessing_section=None, step_section=None):
    cleanup_folder = configuration.get(step_section, "OUTPUT_FOLDER")

    return cleanup_local(dag, upstream_step, cleanup_folder)


def cleanup_local(dag, upstream_step, cleanup_folder):

    cleanup_local_cmd = dedent("""
            rm -rf {{ params["cleanup_folder"] }}/{{ dag_run.conf["session_id"] }}
        """)

    cleanup_local = BashOperator(
        task_id='cleanup_local',
        bash_command=cleanup_local_cmd,
        params={'cleanup_folder': cleanup_folder},
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=1),
        dag=dag
    )

    if upstream_step.task:
        cleanup_local.set_upstream(upstream_step.task)

    cleanup_local.doc_md = dedent("""\
        # Cleanup local files

        Remove locally stored files as they have been already processed.
        """)

    return Step(cleanup_local, 'cleanup_local', upstream_step.priority_weight + 10)
