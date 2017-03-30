"""

Pre processing step: check free space on local disk

Configuration variables used:

* :<pipeline> section
    * MIN_FREE_SPACE
* :<pipeline>:<step> section (first match in a list of steps)
    * OUTPUT_FOLDER

"""

import logging

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow.exceptions import AirflowConfigException
from airflow_freespace.operators import FreeSpaceSensor

from common_steps import Step


def check_local_free_space_cfg(dag, upstream_step, pipeline_section, step_sections):
    min_free_space = configuration.getfloat(pipeline_section, 'MIN_FREE_SPACE')
    local_folder = None
    for step_section in step_sections:
        logging.info("Check folder in %s", step_section)
        try:
            local_folder = configuration.get(step_section, "OUTPUT_FOLDER")
            if local_folder:
                break
        except AirflowConfigException:
            pass

    if not local_folder:
        raise AirflowConfigException('No output folder defined in sections %s' % (','.join(step_sections)))

    return check_local_free_space_step(dag, upstream_step, min_free_space, local_folder)


def check_local_free_space_step(dag, upstream_step, min_free_space, local_folder):

    check_local_free_space = FreeSpaceSensor(
        task_id='check_local_free_space',
        path=local_folder,
        free_disk_threshold=min_free_space,
        retry_delay=timedelta(hours=1),
        retries=24 * 7,
        pool='remote_file_copy',
        dag=dag
    )

    if upstream_step.task:
        check_local_free_space.set_upstream(upstream_step.task)

    check_local_free_space.doc_md = dedent("""\
    # Check free space

    Check that there is at least %.0f%% free space on the disk hosting folder %s for processing, wait otherwise.
    """ % (min_free_space, local_folder))

    return Step(check_local_free_space, check_local_free_space.task_id, upstream_step.priority_weight + 10)
