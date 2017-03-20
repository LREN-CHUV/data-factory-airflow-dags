"""

  Pre processing step: copy files to local folder.

  Input data are first copied to a local folder to speed-up processing.

  Configuration variables used:

  * :preprocessing section
    * INPUT_CONFIG: List of flags defining how incoming imaging data are organised.
    * MIN_FREE_SPACE: minimum percentage of free space available on local disk
  * :preprocessing:copy_to_local section
    * OUTPUT_FOLDER: destination folder for the local copy

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import BashPipelineOperator

from common_steps import Step, default_config


def copy_to_local_cfg(dag, upstream_step, preprocessing_section, step_section):
    default_config(preprocessing_section, 'INPUT_CONFIG', '')

    dataset_config = configuration.get(preprocessing_section, 'INPUT_CONFIG')
    min_free_space = configuration.getfloat(preprocessing_section, 'MIN_FREE_SPACE')
    output_folder = configuration.get(step_section, 'OUTPUT_FOLDER')

    return copy_to_local(dag, upstream_step, min_free_space,
                         output_folder, dataset_config)


def copy_to_local(dag, upstream_step, min_free_space, output_folder,
                  dataset_config):

    copy_to_local_cmd = dedent("""
        used="$(df -h /home | grep '/' | grep -Po '[^ ]*(?=%)')"
        if (( 101 - used < {{ params['min_free_space']|float * 100 }} )); then
          echo "Not enough space left, cannot continue"
          exit 1
        fi
        rsync -av $AIRFLOW_INPUT_DIR/ $AIRFLOW_OUTPUT_DIR/
    """)

    copy_to_local = BashPipelineOperator(
        task_id='copy_to_local',
        bash_command=copy_to_local_cmd,
        params={'min_free_space': min_free_space},
        output_folder_callable=lambda session_id, **kwargs: output_folder + '/' + session_id,
        pool='remote_file_copy',
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=3),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag
    )

    if upstream_step.task:
        copy_to_local.set_upstream(upstream_step.task)

    copy_to_local.doc_md = dedent("""\
    # Copy DICOM files to local %s folder

    Speed-up the processing of DICOM files by first copying them from a shared folder to the local hard-drive.
    """ % output_folder)

    return Step(copy_to_local, 'copy_to_local', upstream_step.priority_weight + 10)
