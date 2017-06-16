"""

Reorganisation step: copy files to local folder.

Input data are first copied to a local folder to speed-up processing.

Configuration variables used:

* :reorganisation section
    * INPUT_CONFIG: List of flags defining how incoming imaging data are organised.
    * MIN_FREE_SPACE: minimum percentage of free space available on local disk
* :reorganisation:copy_to_local section
    * OUTPUT_FOLDER: destination folder for the local copy

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import BashPipelineOperator

from common_steps import Step, default_config


def copy_to_local_cfg(dag, upstream_step, reorganisation_section, step_section):
    default_config(reorganisation_section, 'INPUT_CONFIG', '')

    dataset_config = [flag.strip() for flag in configuration.get(reorganisation_section, 'INPUT_CONFIG').split(',')]
    min_free_space = configuration.getfloat(reorganisation_section, 'MIN_FREE_SPACE')
    output_folder = configuration.get(step_section, 'OUTPUT_FOLDER')

    return copy_to_local_step(dag, upstream_step, min_free_space, output_folder, dataset_config)


def copy_to_local_step(dag, upstream_step, min_free_space, output_folder, dataset_config):

    copy_to_local_cmd = dedent("""
        set -e
        mkdir -p $AIRFLOW_OUTPUT_DIR/
        used="$(df -h $AIRFLOW_OUTPUT_DIR/ | grep '/' | grep -Po '[^ ]*(?=%)')"
        is_full=$(echo "101 - used < {{ params['min_free_space']|float * 100 }}" | bc)
        if [ "$is_full" == 1 ]; then
          echo "Not enough space left, cannot continue"
          exit 1
        fi
        rsync -av $AIRFLOW_INPUT_DIR/ $AIRFLOW_OUTPUT_DIR/
    """)

    copy_to_local = BashPipelineOperator(
        task_id='copy_to_local',
        bash_command=copy_to_local_cmd,
        params={'min_free_space': min_free_space},
        output_folder_callable=lambda relative_context_path, **kwargs: output_folder + '/' + relative_context_path,
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=3),
        dataset_config=dataset_config,
        organised_folder=False,
        dag=dag
    )

    if upstream_step.task:
        copy_to_local.set_upstream(upstream_step.task)

    copy_to_local.doc_md = dedent("""\
        # Copy all DICOM/NIFTI files to a local folder

        Speed-up the processing of DICOM/NIFTI files by first copying them from a shared folder to the local
        hard-drive.

        * Target folder: __%s__

        Depends on: __%s__
    """ % (output_folder, upstream_step.task_id))

    return Step(copy_to_local, copy_to_local.task_id, upstream_step.priority_weight + 10)
