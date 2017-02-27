"""

Pre-process DICOM files in a study folder

"""

import logging
import os

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators import TriggerDagRunOperator
from airflow_spm.operators import SpmPipelineOperator
from airflow_freespace.operators import FreeSpaceSensor
from airflow_pipeline.operators import PreparePipelineOperator, BashPipelineOperator, PythonPipelineOperator
from airflow_pipeline.pipelines import pipeline_trigger

from mri_meta_extract import dicom_import
from mri_meta_extract import nifti_import


def pre_process_dicom_dag(dataset, email_errors_to, max_active_runs, session_id_by_patient, misc_library_path,
                          min_free_space_local_folder, dicom_local_folder,
                          copy_dicom_to_local=True, dicom_files_pattern='**/MR.*',
                          dicom_organizer=False, dicom_organizer_spm_function='dicomOrganizer', dicom_organizer_pipeline_path=None,
                          dicom_organizer_local_folder=None, dicom_organizer_data_structure='PatientID:StudyID:ProtocolName:SeriesNumber',
                          dicom_select_T1=False, dicom_select_T1_spm_function='selectT1', dicom_select_T1_pipeline_path=None,
                          dicom_select_T1_local_folder=None, dicom_select_T1_protocols_file=None,
                          dicom_to_nifti_spm_function='DCM2NII_LREN', dicom_to_nifti_pipeline_path=None,
                          dicom_to_nifti_local_folder=None, dicom_to_nifti_server_folder=None, protocols_file=None, dcm2nii_program=None,
                          mpm_maps=True, mpm_maps_spm_function='Preproc_mpm_maps', mpm_maps_pipeline_path=None,
                          mpm_maps_local_folder=None, mpm_maps_server_folder=None,
                          neuro_morphometric_atlas=True, neuro_morphometric_atlas_spm_function='NeuroMorphometric_pipeline',
                          neuro_morphometric_atlas_pipeline_path=None, neuro_morphometric_atlas_local_folder=None, neuro_morphometric_atlas_server_folder=None,
                          neuro_morphometric_atlas_TPM_template='nwTPM_sl3.nii'):

    # functions used in the DAG

    def extract_dicom_info_fn(folder, session_id, **kwargs):
        """
         Extract the information from DICOM files located inside a folder.
         The folder information should be given in the configuration parameter
         'folder' of the DAG run
        """
        logging.info('folder %s, session_id %s', folder, session_id)

        (participant_id, scan_date) = dicom_import.visit_info(
            folder, files_pattern=dicom_files_pattern)
        dicom_import.dicom2db(folder, files_pattern=dicom_files_pattern)

        return {
            'participant_id': participant_id,
            'scan_date': scan_date
        }

    def dicom_organizer_arguments_fn(folder, session_id, **kwargs):
        """
          Prepare the arguments for conversion pipeline from DICOM to Nifti format.
          It converts all files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')

        return [parent_data_folder,
                dicom_organizer_local_folder,
                session_id,
                dicom_organizer_data_structure]

    def dicom_select_T1_arguments_fn(folder, session_id, **kwargs):
        """
          Prepare the arguments for the pipeline that selects T1 files from DICOM.
          It selects all T1 files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')

        return [parent_data_folder,
                dicom_select_T1_local_folder,
                session_id,
                dicom_select_T1_protocols_file]

    def dicom_to_nifti_arguments_fn(folder, session_id, **kwargs):
        """
          Prepare the arguments for conversion pipeline from DICOM to Nifti format.
          It converts all files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')

        return [parent_data_folder,
                session_id,
                dicom_to_nifti_local_folder,
                dicom_to_nifti_server_folder,
                protocols_file,
                dcm2nii_program]

    def neuro_morphometric_atlas_arguments_fn(folder, session_id, **kwargs):
        """
          Prepare the arguments for the pipeline that builds a Neuro morphometric
          atlas from the Nitfi files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')
        table_format = 'csv'

        return [session_id,
                parent_data_folder,
                neuro_morphometric_atlas_local_folder,
                neuro_morphometric_atlas_server_folder,
                protocols_file,
                table_format,
                neuro_morphometric_atlas_TPM_template]

    def mpm_maps_arguments_fn(folder, session_id, pipeline_params_config_file='Preproc_mpm_maps_pipeline_config.txt', **kwargs):
        """
          Pipeline that builds the MPM maps from the Nitfi files located in the
          folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')

        return [parent_data_folder,
                session_id,
                mpm_maps_local_folder,
                protocols_file,
                pipeline_params_config_file,
                mpm_maps_server_folder]

    def extract_nifti_info_fn(folder, session_id, participant_id, scan_date, **kwargs):
        """
          Extract information from the Nifti files located in the folder 'folder'
          parent_task should contain XCOM keys 'folder' and 'session_id'
        """
        logging.info(
            "NIFTI extract: session_id=%s, input_folder=%s", session_id, folder)
        nifti_import.nifti2db(folder, participant_id, scan_date)
        return "ok"

    # Define the DAG

    DAG_NAME = '%s_mri_pre_process_dicom' % dataset.lower().replace(" ", "_")

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
        path=dicom_local_folder,
        free_disk_threshold=min_free_space_local_folder,
        retry_delay=timedelta(hours=1),
        retries=24 * 7,
        pool='remote_file_copy',
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
        initial_root_folder=dicom_local_folder,
        priority_weight=priority_weight,
        execution_timeout=timedelta(minutes=10),
        dag=dag
    )

    prepare_pipeline.set_upstream(upstream)

    prepare_pipeline.doc_md = dedent("""\
    # Prepare pipeline

    Add information required by the SpmPipeline operators.
    """)

    upstream = prepare_pipeline
    upstream_id = 'prepare_pipeline'
    priority_weight = priority_weight + 5

    if copy_dicom_to_local:

        copy_dicom_to_local_cmd = dedent("""
            used="$(df -h /home | grep '/' | grep -Po '[^ ]*(?=%)')"
            if (( 101 - used < {{ params['min_free_space_local_folder']|float * 100 }} )); then
              echo "Not enough space left, cannot continue"
              exit 1
            fi
            rsync -av $AIRFLOW_INPUT_FOLDER/ $AIRFLOW_OUTPUT_FOLDER/
        """)

        copy_dicom_to_local = BashPipelineOperator(
            task_id='copy_dicom_to_local',
            bash_command=copy_dicom_to_local_cmd,
            params={'min_free_space_local_folder': min_free_space_local_folder},
            output_folder_callable=lambda session_id, **kwargs: dicom_local_folder + '/' + session_id,
            pool='remote_file_copy',
            parent_task=upstream_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=3),
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            session_id_by_patient=session_id_by_patient,
            dag=dag
        )
        copy_dicom_to_local.set_upstream(upstream)

        copy_dicom_to_local.doc_md = dedent("""\
        # Copy DICOM files to local %s folder

        Speed-up the processing of DICOM files by first copying them from a shared folder to the local hard-drive.
        """ % dicom_local_folder)

        upstream = copy_dicom_to_local
        upstream_id = 'copy_dicom_to_local'
        priority_weight = priority_weight + 5

    else:

        # Register local data into the Data catalog/provenance tables

        register_local_cmd = ";"

        register_local = BashPipelineOperator(
            task_id='register_local',
            bash_command=register_local_cmd,
            params={},
            parent_task=upstream_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=3),
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            session_id_by_patient=session_id_by_patient,
            dag=dag
        )
        register_local.set_upstream(upstream)

        register_local.doc_md = dedent("""\
        # Register incoming files for provenance

        This step does nothing except register the files in the input folder for provenance.
        """ % dicom_local_folder)

        upstream = register_local
        upstream_id = 'register_local'
        priority_weight = priority_weight + 5

    # endif

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
