"""

  Pre processing step: check free space on local disk

  Configuration variables used:

  * MIN_FREE_SPACE_LOCAL_FOLDER
  * <local_folder_config_key>

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_freespace.operators import FreeSpaceSensor

from common_steps import Step


def check_free_space_local_cfg(dag, upstream_step, dataset_section, local_folder_config_key):
    min_free_space_local_folder = configuration.getfloat(dataset_section, 'MIN_FREE_SPACE_LOCAL_FOLDER')
    local_folder = configuration.get(dataset_section, local_folder_config_key)

    return check_free_space_local(dag, upstream_step, min_free_space_local_folder,
                                  local_folder)


def check_free_space_local(dag, upstream_step, min_free_space_local_folder, local_folder):

    check_free_space = FreeSpaceSensor(
        task_id='check_free_space',
        path=local_folder,
        free_disk_threshold=min_free_space_local_folder,
        retry_delay=timedelta(hours=1),
        retries=24 * 7,
        pool='remote_file_copy',
        dag=dag
    )

    if upstream_step.task:
        check_free_space.set_upstream(upstream_step.task)

    check_free_space.doc_md = dedent("""\
    # Check free space

    Check that there is enough free space on the disk hosting folder %s for processing, wait otherwise.
    """ % local_folder)

    return Step(check_free_space, 'check_free_space', upstream_step.priority_weight + 10)
