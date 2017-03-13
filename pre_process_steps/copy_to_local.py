"""

  Pre processing step: copy files to local

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import BashPipelineOperator


def copy_to_local_cfg(upstream, upstream_id, priority_weight, dataset_section, local_folder_config_key):
    min_free_space_local_folder = configuration.getfloat(dataset_section, 'MIN_FREE_SPACE_LOCAL_FOLDER')
    copy_to_local_folder = configuration.get(dataset_section, local_folder_config_key)
    dataset_config = configuration.get(dataset_section, 'DATASET_CONFIG')

    return copy_to_local(upstream, upstream_id, priority_weight, min_free_space_local_folder, copy_to_local_folder, dataset_config)


def copy_to_local(upstream, upstream_id, priority_weight, min_free_space_local_folder, copy_to_local_folder, dataset_config):

    copy_to_local_cmd = dedent("""
        used="$(df -h /home | grep '/' | grep -Po '[^ ]*(?=%)')"
        if (( 101 - used < {{ params['min_free_space_local_folder']|float * 100 }} )); then
          echo "Not enough space left, cannot continue"
          exit 1
        fi
        rsync -av $AIRFLOW_INPUT_DIR/ $AIRFLOW_OUTPUT_DIR/
    """)

    copy_to_local = BashPipelineOperator(
        task_id='copy_to_local',
        bash_command=copy_to_local_cmd,
        params={'min_free_space_local_folder': min_free_space_local_folder},
        output_folder_callable=lambda session_id, **kwargs: copy_to_local_folder + '/' + session_id,
        pool='remote_file_copy',
        parent_task=upstream_id,
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=3),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag
    )
    copy_to_local.set_upstream(upstream)

    copy_to_local.doc_md = dedent("""\
    # Copy DICOM files to local %s folder

    Speed-up the processing of DICOM files by first copying them from a shared folder to the local hard-drive.
    """ % copy_to_local_folder)

    upstream = copy_to_local
    upstream_id = 'copy_to_local'
    priority_weight += 10

    return (upstream, upstream_id, priority_weight)
