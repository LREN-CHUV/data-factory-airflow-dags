"""

  Pre processing step: copy files to local

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_freespace.operators import FreeSpaceSensor


def check_free_space_local_cfg(upstream, upstream_id, priority_weight, dataset_section):
    min_free_space_local_folder = configuration.getfloat(dataset_section, 'MIN_FREE_SPACE_LOCAL_FOLDER')
    copy_to_local_folder = configuration.get(dataset_section, 'COPY_TO_LOCAL_FOLDER')
    dataset_config = configuration.get(dataset_section, 'DATASET_CONFIG')

    return check_free_space_local(upstream, upstream_id, priority_weight, min_free_space_local_folder, copy_to_local_folder, dataset_config)


def check_free_space_local(upstream, upstream_id, priority_weight, min_free_space_local_folder, copy_to_local_folder):

    check_free_space = FreeSpaceSensor(
        task_id='check_free_space',
        path=copy_to_local_folder,
        free_disk_threshold=min_free_space_local_folder,
        retry_delay=timedelta(hours=1),
        retries=24 * 7,
        pool='remote_file_copy',
        dag=dag
    )

    check_free_space.doc_md = dedent("""\
    # Check free space

    Check that there is enough free space on the disk hosting folder %s for processing, wait otherwise.
    """ % copy_to_local_folder)

    upstream = check_free_space
    upstream_id = 'check_free_space'
    priority_weight += 10

    return (upstream, upstream_id, priority_weight)
