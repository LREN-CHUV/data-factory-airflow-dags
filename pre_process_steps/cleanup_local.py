"""

  Pre processing step: cleanup local

  Configuration variables used:

  * <local_folder_config_key>

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow.operators import BashOperator

from common_steps import Step


def cleanup_local_cfg(dag, upstream_step, dataset_section, local_folder_config_key):
    copy_to_local_folder = configuration.get(dataset_section, local_folder_config_key)

    return cleanup_local(dag, upstream_step, copy_to_local_folder)


def cleanup_local(dag, upstream_step, copy_to_local_folder):

    cleanup_local_cmd = dedent("""
            rm -rf {{ params["local_folder"] }}/{{ dag_run.conf["session_id"] }}
        """)

    cleanup_local = BashOperator(
        task_id='cleanup_local',
        bash_command=cleanup_local_cmd,
        params={'local_folder': copy_to_local_folder},
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=1),
        dag=dag
    )

    if upstream_step.task:
        cleanup_local.set_upstream(upstream_step.task)

    cleanup_local.doc_md = dedent("""\
        # Cleanup local files

        Remove locally stored files as they have been processed already.
        """)

    return Step(cleanup_local, 'cleanup_local', upstream_step.priority_weight + 10)
