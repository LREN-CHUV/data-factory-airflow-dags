"""

ETL steps: version incoming EHR

Copy files to a versioned folder.

Configuration variables used:

* :ehr section:
    * MIN_FREE_SPACE
* :ehr:version_incoming_ehr section:
    * OUTPUT_FOLDER

"""

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import BashPipelineOperator

from common_steps import Step


def version_incoming_ehr_pipeline_cfg(dag, upstream_step, ehr_section, step_section):
    min_free_space = configuration.get(ehr_section, 'MIN_FREE_SPACE')
    output_folder = configuration.get(step_section, 'OUTPUT_FOLDER')

    return version_incoming_ehr_pipeline_step(dag, upstream_step, output_folder, min_free_space)


def version_incoming_ehr_pipeline_step(dag, upstream_step, output_folder=None, min_free_space=None):

    version_incoming_ehr_cmd = dedent("""
            export HOME=/usr/local/airflow
            mkdir -p {{ params['ehr_versioned_folder'] }}
            [ -d {{ params['ehr_versioned_folder'] }}/.git ] || git init {{ params['ehr_versioned_folder'] }}
            rsync -av $AIRFLOW_INPUT_DIR/ $AIRFLOW_OUTPUT_DIR/
            cd {{ params['ehr_versioned_folder'] }}
            git add $AIRFLOW_OUTPUT_DIR/
            CONTEXT="{{ task_instance.xcom_pull(key='relative_context_path', task_ids='prepare_pipeline') }}"
            git commit -m "Add EHR acquired on $CONTEXT"
            git rev-parse HEAD
        """)

    version_incoming_ehr_pipeline = BashPipelineOperator(
        task_id='version_incoming_ehr_pipeline',
        bash_command=version_incoming_ehr_cmd,
        params={'min_free_space_local_folder': min_free_space,
                'ehr_versioned_folder': output_folder
                },
        output_folder_callable=lambda relative_context_path, **kwargs: "%s/%s" % (
            output_folder, relative_context_path),
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(minutes=30),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dag=dag
    )

    if upstream_step.task:
        version_incoming_ehr_pipeline.set_upstream(upstream_step.task)

    version_incoming_ehr_pipeline.doc_md = dedent("""\
    # Copy EHR files to a versioned folder

    * Target folder: __%s__

    The folder %s is versioned as a local Git repository.
    """ % (output_folder, output_folder))

    return Step(version_incoming_ehr_pipeline, version_incoming_ehr_pipeline.task_id,
                upstream_step.priority_weight + 10)
