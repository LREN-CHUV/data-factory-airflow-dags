"""

  Pre processing step: cleanup local

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow.operators import BashOperator


def cleanup_cfg(upstream, upstream_id, priority_weight, dataset_section, local_folder_config_key):
    copy_to_local_folder = configuration.get(dataset_section, local_folder_config_key)

    return cleanup_local(upstream, upstream_id, priority_weight, copy_to_local_folder)


def cleanup_local(upstream, upstream_id, priority_weight, min_free_space_local_folder, copy_to_local_folder):

    cleanup_local_cmd = dedent("""
            rm -rf {{ params["local_folder"] }}/{{ dag_run.conf["session_id"] }}
        """)

    cleanup_local = BashOperator(
        task_id='cleanup_local',
        bash_command=cleanup_local_cmd,
        params={'local_folder': copy_to_local_folder},
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=1),
        dag=dag
    )
    cleanup_local.set_upstream(upstream)

    cleanup_local.doc_md = dedent("""\
        # Cleanup local files

        Remove locally stored files as they have been processed already.
        """)

    upstream = copy_to_local
    upstream_id = 'copy_to_local'
    priority_weight += 5

    return (upstream, upstream_id, priority_weight)
