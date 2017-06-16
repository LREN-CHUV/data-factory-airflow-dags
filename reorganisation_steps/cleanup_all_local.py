"""

Reorganisation step: cleanup all local data.

Cleanup the local data (for the whole data-set) created during copy_to_local step.

Configuration variables used:

* :reorganisation:copy_to_local section
    * OUTPUT_FOLDER: destination folder for the local copy

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow.operators.bash_operator import BashOperator

from common_steps import Step


def cleanup_all_local_cfg(dag, upstream_step, step_section=None):
    cleanup_folder = configuration.get(step_section, "OUTPUT_FOLDER")

    return cleanup_all_local_step(dag, upstream_step, cleanup_folder)


def cleanup_all_local_step(dag, upstream_step, cleanup_folder):

    cleanup_local_cmd = dedent("""
            rm -rf {{ params["cleanup_folder"] }}/*
        """)

    cleanup_all_local = BashOperator(
        task_id='cleanup_all_local',
        bash_command=cleanup_local_cmd,
        params={'cleanup_folder': cleanup_folder},
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=1),
        dag=dag
    )

    if upstream_step.task:
        cleanup_all_local.set_upstream(upstream_step.task)

    cleanup_all_local.doc_md = dedent("""\
        # Cleanup all local files

        Remove locally stored files as they have been already reorganised.
        """)

    return Step(cleanup_all_local, cleanup_all_local.task_id, upstream_step.priority_weight + 10)
