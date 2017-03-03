"""

Take EHR data located in a study folder and convert it to I2B2

Poll a base directory for incoming CSV files ready for processing. We assume that
CSV files are already anonymised and organised with the following directory structure:

  2016
     _ 20160407
        _ PR01471_CC082251
           _ patients.csv
           _ diseases.csv
           _ ...

"""

import os
import logging

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators import BashOperator, TriggerDagRunOperator
from airflow_freespace.operators import FreeSpaceSensor
from airflow_pipeline.operators import PreparePipelineOperator, BashPipelineOperator, DockerPipelineOperator
from airflow_pipeline.pipelines import pipeline_trigger


def ehr_to_i2b2_dag(dataset, email_errors_to, max_active_runs, min_free_space_local_folder,
                    mipmap_db_confile_file, ehr_versioned_folder,
                    ehr_to_i2b2_capture_docker_image, ehr_to_i2b2_capture_folder):

    # constants

    DAG_NAME = '%s_ehr_to_i2b2' % dataset.lower().replace(" ", "_")

    # Define the DAG

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.now(),
        'retries': 1,
        'retry_delay': timedelta(seconds=120),
        'email': email_errors_to,
        'email_on_failure': True,
        'email_on_retry': True
    }

    dag = DAG(
        dag_id=DAG_NAME,
        default_args=default_args,
        schedule_interval=None,
        max_active_runs=max_active_runs)

    check_free_space = FreeSpaceSensor(
        task_id='check_free_space',
        path=ehr_versioned_folder,
        free_disk_threshold=min_free_space_local_folder,
        retry_delay=timedelta(hours=1),
        retries=24 * 7,
        dag=dag
    )

    check_free_space.doc_md = dedent("""\
    # Check free space

    Check that there is enough free space on the disk hosting folder %s for processing, wait otherwise.
    """ % ehr_versioned_folder)

    upstream = check_free_space
    upstream_id = 'check_free_space'
    priority_weight = 10

    prepare_pipeline = PreparePipelineOperator(
        task_id='prepare_pipeline',
        include_spm_facts=False,
        priority_weight=priority_weight,
        execution_timeout=timedelta(minutes=10),
        dag=dag
    )

    prepare_pipeline.set_upstream(upstream)

    prepare_pipeline.doc_md = dedent("""\
    # Prepare pipeline

    Add information required by the Pipeline operators.
    """)

    upstream = prepare_pipeline
    upstream_id = 'prepare_pipeline'
    priority_weight = priority_weight + 5

    version_incoming_ehr_cmd = dedent("""
        mkdir -p {{ params['ehr_versioned_folder'] }}
        [ -d {{ params['ehr_versioned_folder'] }}/.git ] || git init {{ params['ehr_versioned_folder'] }}
        rsync -av $AIRFLOW_INPUT_DIR/ $AIRFLOW_OUTPUT_DIR/
        cd {{ params['ehr_versioned_folder'] }}
        git add $AIRFLOW_OUTPUT_DIR/
        git commit -m "Add EHR acquired on {{ task_instance.xcom_pull(key='relative_context_path', task_ids='prepare_pipeline') }}"
        git rev-parse HEAD
    """)

    version_incoming_ehr = BashPipelineOperator(
        task_id='version_incoming_ehr',
        bash_command=version_incoming_ehr_cmd,
        params={'min_free_space_local_folder': min_free_space_local_folder,
                'ehr_versioned_folder': ehr_versioned_folder
                },
        output_folder_callable=lambda relative_context_path, **kwargs: "%s/%s" % (
            ehr_versioned_folder, relative_context_path),
        parent_task=upstream_id,
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=3),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dag=dag
    )
    version_incoming_ehr.set_upstream(upstream)

    version_incoming_ehr.doc_md = dedent("""\
    # Copy EHR files to local %s folder

    Speed-up the processing of DICOM files by first copying them from a shared folder to the local hard-drive.
    """ % ehr_versioned_folder)

    upstream = version_incoming_ehr
    upstream_id = 'version_incoming_ehr'
    priority_weight = priority_weight + 5

    # Next: Python to build provenance_details

    # Next: call MipMap on versioned folder

    map_ehr_to_i2b2_capture = DockerPipelineOperator(
        task_id='map_ehr_to_i2b2_capture',
        image=ehr_to_i2b2_capture_docker_image,
        force_pull=False,
        command=None,
        environment=None,
        cpus=1,
        mem_limit='256m',
        container_tmp_dir='/tmp/airflow',
        container_input_dir='/inputs',
        container_output_dir='/outputs',
        output_folder_callable=lambda relative_context_path, **kwargs: "%s/%s" % (
            ehr_to_i2b2_capture_folder, relative_context_path),
        user=None,
        volumes=None,
        pool='io_intensive',
        parent_task=upstream_id,
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=24),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dag=dag
    )

    map_ehr_to_i2b2_capture.set_upstream(upstream)

    map_ehr_to_i2b2_capture.doc_md = dedent("""\
    # MipMap ETL: map EHR data to I2B2

    Docker image: __%s__

    * Local folder: __%s__

    Depends on: __%s__
    """ % (ehr_to_i2b2_capture_docker_image, ehr_to_i2b2_capture_folder, upstream_id))

    upstream = map_ehr_to_i2b2_capture
    upstream_id = 'map_ehr_to_i2b2_capture'
    priority_weight = priority_weight + 5

    return dag
