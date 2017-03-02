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

def ehr_to_i2b2_dag(dataset, email_errors_to, max_active_runs,
                          min_free_space_local_folder, ehr_versioned_folder):

    # constants

    START = datetime.utcnow()
    START = datetime.combine(START.date(), time(START.hour, 0))

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
    """ % dicom_local_folder)

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
        output_folder_callable=lambda relative_context_path, **kwargs: "%s/%s" % (ehr_versioned_folder, relative_context_path),
        parent_task=upstream_id,
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=3),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        session_id_by_patient=session_id_by_patient,
        dag=dag
    )
    version_incoming_ehr.set_upstream(upstream)

    version_incoming_ehr.doc_md = dedent("""\
    # Copy EHR files to local %s folder

    Speed-up the processing of DICOM files by first copying them from a shared folder to the local hard-drive.
    """ % dicom_local_folder)

    upstream = version_incoming_ehr
    upstream_id = 'version_incoming_ehr'
    priority_weight = priority_weight + 5

    # Next: Python to build provenance_details

    # Next: call MipMap on versioned folder

    if dicom_organizer:

        dicom_organizer_pipeline = SpmPipelineOperator(
            task_id='dicom_organizer_pipeline',
            spm_function=dicom_organizer_spm_function,
            spm_arguments_callable=dicom_organizer_arguments_fn,
            matlab_paths=[misc_library_path, dicom_organizer_pipeline_path],
            output_folder_callable=lambda session_id, **kwargs: dicom_organizer_local_folder + '/' + session_id,
            pool='io_intensive',
            parent_task=upstream_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=24),
            on_skip_trigger_dag_id='mri_notify_skipped_processing',
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            session_id_by_patient=session_id_by_patient,
            dag=dag
        )

        dicom_organizer_pipeline.set_upstream(upstream)

        dicom_organizer_pipeline.doc_md = dedent("""\
        # DICOM organizer pipeline

        SPM function: __%s__

        Reorganise DICOM files to fit the structure expected by the following pipelines.

        Reorganised DICOM files are stored the the following locations:

        * Local folder: __%s__

        Depends on: __%s__
        """ % (dicom_organizer_spm_function, dicom_organizer_local_folder, upstream_id))

        upstream = dicom_organizer_pipeline
        upstream_id = 'dicom_organizer_pipeline'
        priority_weight = priority_weight + 5

    # endif

    if dicom_select_T1:

        dicom_select_T1_pipeline = SpmPipelineOperator(
            task_id='dicom_select_T1_pipeline',
            spm_function=dicom_select_T1_spm_function,
            spm_arguments_callable=dicom_select_T1_arguments_fn,
            matlab_paths=[misc_library_path, dicom_select_T1_pipeline_path],
            output_folder_callable=lambda session_id, **kwargs: dicom_select_T1_local_folder + '/' + session_id,
            pool='io_intensive',
            parent_task=upstream_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=24),
            on_skip_trigger_dag_id='mri_notify_skipped_processing',
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            session_id_by_patient=session_id_by_patient,
            dag=dag
        )

        dicom_select_T1_pipeline.set_upstream(upstream)

        dicom_select_T1_pipeline.doc_md = dedent("""\
        # select T1 DICOM pipeline

        SPM function: __%s__

        Selects only T1 images from a set of various DICOM images.

        Selected DICOM files are stored the the following locations:

        * Local folder: __%s__

        Depends on: __%s__
        """ % (dicom_select_T1_spm_function, dicom_select_T1_local_folder, upstream_id))

        upstream = dicom_select_T1_pipeline
        upstream_id = 'dicom_select_T1_pipeline'
        priority_weight = priority_weight + 5

    # endif

    extract_dicom_info = PythonPipelineOperator(
        task_id='extract_dicom_info',
        python_callable=extract_dicom_info_fn,
        parent_task=upstream_id,
        pool='io_intensive',
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=6),
        dag=dag
    )
    extract_dicom_info.set_upstream(upstream)

    extract_dicom_info.doc_md = dedent("""\
    # Extract DICOM information

    Read DICOM information from the files stored in the session folder and store that information into the database.
    """)

    # This upstream is required as we need to extract additional information from the DICOM files (participant_id, scan_date)
    # and transfer it via XCOMs to the next SPM pipeline
    upstream = extract_dicom_info
    upstream_id = 'extract_dicom_info'
    priority_weight = priority_weight + 5

    dicom_to_nifti_pipeline = SpmPipelineOperator(
        task_id='dicom_to_nifti_pipeline',
        spm_function=dicom_to_nifti_spm_function,
        spm_arguments_callable=dicom_to_nifti_arguments_fn,
        matlab_paths=[misc_library_path, dicom_to_nifti_pipeline_path],
        output_folder_callable=lambda session_id, **kwargs: dicom_to_nifti_local_folder + '/' + session_id,
        pool='io_intensive',
        parent_task=upstream_id,
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=24),
        on_skip_trigger_dag_id='mri_notify_skipped_processing',
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        session_id_by_patient=session_id_by_patient,
        dag=dag
    )

    dicom_to_nifti_pipeline.set_upstream(upstream)

    dicom_to_nifti_pipeline.doc_md = dedent("""\
    # DICOM to Nitfi Pipeline

    SPM function: __%s__

    This function convert the dicom files to Nifti format using the SPM tools and
    [dcm2nii](http://www.mccauslandcenter.sc.edu/mricro/mricron/dcm2nii.html) tool developed by Chris Rorden.

    Nifti files are stored the the following locations:

    * Local folder: __%s__
    * Remote folder: __%s__

    Depends on: __%s__
    """ % (dicom_to_nifti_spm_function, dicom_to_nifti_local_folder, dicom_to_nifti_server_folder, upstream_id))

    upstream = dicom_to_nifti_pipeline
    upstream_id = 'dicom_to_nifti_pipeline'
    priority_weight = priority_weight + 5

    cleanup_local_dicom_cmd = dedent("""
        rm -rf {{ params["local_folder"] }}/{{ dag_run.conf["session_id"] }}
    """)

    cleanup_local_dicom = BashOperator(
        task_id='cleanup_local_dicom',
        bash_command=cleanup_local_dicom_cmd,
        params={'local_folder': dicom_local_folder},
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=1),
        dag=dag
    )
    cleanup_local_dicom.set_upstream(upstream)
    priority_weight = priority_weight + 5

    cleanup_local_dicom.doc_md = dedent("""\
    # Cleanup local DICOM files

    Remove locally stored DICOM files as they have been processed already.
    """)


    extract_nifti_info = PythonPipelineOperator(
        task_id='extract_nifti_info',
        python_callable=extract_nifti_info_fn,
        parent_task=upstream_id,
        pool='io_intensive',
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=3),
        dag=dag
    )

    extract_nifti_info.set_upstream(dicom_to_nifti_pipeline)
    priority_weight = priority_weight + 5

    extract_nifti_info.doc_md = dedent("""\
    # Extract information from NIFTI files converted from DICOM

    Read NIFTI information from directory %s containing nifti files freshly converted from DICOM and store that information into the database.
    """ % dicom_to_nifti_local_folder)

    notify_success = TriggerDagRunOperator(
        task_id='notify_success',
        trigger_dag_id='mri_notify_successful_processing',
        python_callable=pipeline_trigger('extract_nifti_atlas_info'),
        priority_weight=999,
        dag=dag
    )

    notify_success.set_upstream(extract_nifti_info)

    notify_success.doc_md = dedent("""\
    # Notify successful processing

    Notify successful processing of this MRI scan session.
    """)

    if mpm_maps:

        mpm_maps_pipeline = SpmPipelineOperator(
            task_id='mpm_maps_pipeline',
            spm_function=mpm_maps_spm_function,
            spm_arguments_callable=mpm_maps_arguments_fn,
            matlab_paths=[misc_library_path, mpm_maps_pipeline_path],
            output_folder_callable=lambda session_id, **kwargs: mpm_maps_local_folder + '/' + session_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=24),
            pool='image_preprocessing',
            parent_task=upstream_id,
            on_skip_trigger_dag_id='mri_notify_skipped_processing',
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            session_id_by_patient=session_id_by_patient,
            dag=dag
        )

        mpm_maps_pipeline.set_upstream(upstream)

        mpm_maps_pipeline.doc_md = dedent("""\
        # MPM Maps Pipeline

        SPM function: __%s__

        This function computes the Multiparametric Maps (MPMs) (R2*, R1, MT, PD) and brain segmentation in different tissue maps.
        All computation was programmed based on the LREN database structure.

        The MPMs are calculated locally and finally copied to a remote folder:

        * Local folder: __%s__
        * Remote folder: __%s__

        Depends on: __%s__
        """ % (mpm_maps_spm_function, mpm_maps_local_folder, mpm_maps_server_folder, upstream_id))

        upstream = mpm_maps_pipeline
        upstream_id = 'mpm_maps_pipeline'
        priority_weight = priority_weight + 5

        extract_nifti_mpm_info = PythonPipelineOperator(
            task_id='extract_nifti_mpm_info',
            python_callable=extract_nifti_info_fn,
            parent_task=upstream_id,
            pool='io_intensive',
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=3),
            dag=dag
        )

        extract_nifti_mpm_info.set_upstream(upstream)

        extract_nifti_mpm_info.doc_md = dedent("""\
        # Extract information from NIFTI files generated by MPM pipeline

        Read NIFTI information from directory %s containing the Nifti files created by MPM pipeline and store that information in the database.
        """ % mpm_maps_local_folder)

        notify_success.set_upstream(extract_nifti_mpm_info)
        priority_weight = priority_weight + 5

    # endif

    if neuro_morphometric_atlas:

        neuro_morphometric_atlas_pipeline = SpmPipelineOperator(
            task_id='neuro_morphometric_atlas_pipeline',
            spm_function=neuro_morphometric_atlas_spm_function,
            spm_arguments_callable=neuro_morphometric_atlas_arguments_fn,
            matlab_paths=[misc_library_path,
                          neuro_morphometric_atlas_pipeline_path,
                          mpm_maps_pipeline_path],
            output_folder_callable=lambda session_id, **kwargs: neuro_morphometric_atlas_local_folder + '/' + session_id,
            pool='image_preprocessing',
            parent_task=upstream_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=24),
            on_skip_trigger_dag_id='mri_notify_skipped_processing',
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            session_id_by_patient=session_id_by_patient,
            dag=dag
        )

        neuro_morphometric_atlas_pipeline.set_upstream(upstream)
        priority_weight = priority_weight + 5

        neuro_morphometric_atlas_pipeline.doc_md = dedent("""\
        # NeuroMorphometric Pipeline

        SPM function: __%s__

        This function computes an individual Atlas based on the NeuroMorphometrics Atlas. This is based on the NeuroMorphometrics Toolbox.
        This delivers three files:

        1. Atlas File (*.nii);
        2. Volumes of the Morphometric Atlas structures (*.txt);
        3. CSV File (.csv) containing the volume, globals, and Multiparametric Maps (R2*, R1, MT, PD) for each structure defined in the Subject Atlas.

        The atlas is calculated locally and finally copied to a remote folder:

        * Local folder: %s
        * Remote folder: %s

        Depends on: __%s__
        """ % (neuro_morphometric_atlas_spm_function, neuro_morphometric_atlas_local_folder, neuro_morphometric_atlas_server_folder, upstream_id))

        extract_nifti_atlas_info = PythonPipelineOperator(
            task_id='extract_nifti_atlas_info',
            python_callable=extract_nifti_info_fn,
            parent_task='neuro_morphometric_atlas_pipeline',
            pool='io_intensive',
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=3),
            dag=dag
        )

        extract_nifti_atlas_info.set_upstream(
            neuro_morphometric_atlas_pipeline)

        extract_nifti_atlas_info.doc_md = dedent("""\
        # Extract information from NIFTI files generated by Neuro Morphometrics Atlas pipeline

        Read NIFTI information from directory %s containing the Nifti files created by Neuro Morphometrics Atlas pipeline and store that information in the database.
        """ % neuro_morphometric_atlas_local_folder)

        notify_success.set_upstream(extract_nifti_atlas_info)
        priority_weight = priority_weight + 5

    # endif

    return dag
