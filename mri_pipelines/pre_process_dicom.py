"""

Pre-process DICOM files in a study folder

"""

import logging
import os

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators import BashOperator, TriggerDagRunOperator
from airflow_spm.operators import SpmPipelineOperator
from airflow_pipeline.operators import PreparePipelineOperator, PythonPipelineOperator
from airflow_pipeline.pipelines import pipeline_trigger

from mri_meta_extract.files_recording import create_provenance
from mri_meta_extract.files_recording import visit

from i2b2_import import features_csv_import

from pre_process_steps.check_free_space_local import check_free_space_local_cfg
from pre_process_steps.copy_to_local import copy_to_local_cfg
from pre_process_steps.register_local import register_local_cfg
from pre_process_steps.images_organizer import images_organizer_cfg

def pre_process_dicom_dag(dataset, dataset_section, email_errors_to, max_active_runs,
                          misc_library_path, copy_to_local_folder,
                          dataset_config=None, copy_to_local=True, images_organizer=False,
                          images_selection=False, images_selection_local_folder=None, images_selection_csv_path=None,
                          dicom_select_t1=False, dicom_select_t1_spm_function='selectT1',
                          dicom_select_t1_pipeline_path=None, dicom_select_t1_local_folder=None,
                          dicom_select_t1_protocols_file=None, dicom_to_nifti_spm_function='DCM2NII_LREN',
                          dicom_to_nifti_pipeline_path=None, dicom_to_nifti_local_folder=None,
                          dicom_to_nifti_server_folder=None, protocols_file=None, dcm2nii_program=None, mpm_maps=True,
                          mpm_maps_spm_function='Preproc_mpm_maps', mpm_maps_pipeline_path=None,
                          mpm_maps_local_folder=None, mpm_maps_server_folder=None, neuro_morphometric_atlas=True,
                          neuro_morphometric_atlas_spm_function='NeuroMorphometric_pipeline',
                          neuro_morphometric_atlas_pipeline_path=None, neuro_morphometric_atlas_local_folder=None,
                          neuro_morphometric_atlas_server_folder=None,
                          neuro_morphometric_atlas_tpm_template='nwTPM_sl3.nii', import_features_local_folder=None):

    # functions used in the DAG

    def extract_images_info_fn(folder, session_id, step_name, software_versions=None, **kwargs):
        """
         Extract the information from DICOM/NIFTI files located inside a folder.
         The folder information should be given in the configuration parameter
         'folder' of the DAG run
        """
        logging.info('folder %s, session_id %s', folder, session_id)

        provenance = create_provenance(dataset, software_versions=software_versions)
        step = visit(folder, provenance, step_name, config=dataset_config)

        return step

    def images_selection_fn(folder, session_id, **kwargs):
        """
          Selects files from DICOM/NIFTI that match criterion in CSV file.
          It selects all files located in the folder 'folder' matching criterion in CSV file
        """
        import csv
        from glob import iglob
        from os import makedirs
        from os import listdir
        from os.path import join
        from shutil import copy2

        with open(images_selection_csv_path, mode='r', newline='') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',')
            for row in filereader:
                for folder in iglob(join(folder, row[0], "**/", row[1]), recursive=True):
                    path_elements = folder.split('/')
                    repetition_folder = join(images_selection_local_folder, row[0], path_elements[-3],
                                             path_elements[-2], row[1])
                    makedirs(repetition_folder, exist_ok=True)
                    for file_ in listdir(folder):
                        copy2(join(folder, file_), join(repetition_folder, file_))

        return "ok"

    def dicom_select_t1_arguments_fn(folder, session_id, **kwargs):
        """
          Prepare the arguments for the pipeline that selects T1 files from DICOM.
          It selects all T1 files located in the folder 'folder'
        """
        parent_data_folder = os.path.abspath(folder + '/..')

        return [parent_data_folder,
                dicom_select_t1_local_folder,
                session_id,
                dicom_select_t1_protocols_file]

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
                neuro_morphometric_atlas_tpm_template]

    def mpm_maps_arguments_fn(folder, session_id, pipeline_params_config_file='Preproc_mpm_maps_pipeline_config.txt',
                              **kwargs):
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

    def features_to_i2b2_fn(folder, i2b2_conn, **kwargs):
        """
          Import brain features from CSV files to I2B2 DB
        """
        features_csv_import.folder2db(folder, i2b2_conn, dataset, dataset_config)

        return "ok"

    # Define the DAG

    dag_name = '%s_mri_pre_process_dicom' % dataset.lower().replace(" ", "_")

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
        dag_id=dag_name,
        default_args=default_args,
        schedule_interval=None,
        max_active_runs=max_active_runs)

    upstream, upstream_id, priority_weight = check_free_space_local_cfg(dag, None, None, 0, dataset_section,
                                                                        "DICOM_LOCAL_FOLDER")

    prepare_pipeline = PreparePipelineOperator(
        task_id='prepare_pipeline',
        include_spm_facts=True,
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
    priority_weight += 5

    if copy_to_local:
        upstream, upstream_id, priority_weight = copy_to_local_cfg(dag, upstream, upstream_id, priority_weight,
                                                                   dataset_section, "DICOM_LOCAL_FOLDER")
    else:
        upstream, upstream_id, priority_weight = register_local_cfg(dag, upstream, upstream_id, priority_weight,
                                                                    dataset_section)
    # endif

    if images_organizer:
        upstream, upstream_id, priority_weight = images_organizer_cfg(dag, upstream, upstream_id, priority_weight,
                                                                      dataset_section, dataset, "DICOM_LOCAL_FOLDER")
    # endif

    if images_selection:

        images_selection_pipeline = PythonPipelineOperator(
            task_id='images_selection_pipeline',
            python_callable=images_selection_fn,
            output_folder_callable=lambda session_id, **kwargs: images_selection_local_folder + '/' + session_id,
            pool='io_intensive',
            parent_task=upstream_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=6),
            on_skip_trigger_dag_id='mri_notify_skipped_processing',
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            dag=dag
        )

        images_selection_pipeline.set_upstream(upstream)

        images_selection_pipeline.doc_md = dedent("""\
        # select DICOM/NIFTI pipeline

        Selects only images matching criterion defined in a CSV file from a set of various DICOM/NIFTI images. For
        example we might want to keep only the baseline visits and T1 images.

        Selected DICOM/NIFTI files are stored the the following locations:

        * Local folder: __%s__

        Depends on: __%s__
        """ % (images_selection_local_folder, upstream_id))

        upstream = images_selection_pipeline
        upstream_id = 'images_selection_pipeline'
        priority_weight += 5

    # endif

    if dicom_select_t1:

        dicom_select_t1_pipeline = SpmPipelineOperator(
            task_id='dicom_select_T1_pipeline',
            spm_function=dicom_select_t1_spm_function,
            spm_arguments_callable=dicom_select_t1_arguments_fn,
            matlab_paths=[misc_library_path, dicom_select_t1_pipeline_path],
            output_folder_callable=lambda session_id, **kwargs: dicom_select_t1_local_folder + '/' + session_id,
            pool='io_intensive',
            parent_task=upstream_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=24),
            on_skip_trigger_dag_id='mri_notify_skipped_processing',
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            dataset_config=dataset_config,
            dag=dag
        )

        dicom_select_t1_pipeline.set_upstream(upstream)

        dicom_select_t1_pipeline.doc_md = dedent("""\
        # select T1 DICOM pipeline

        SPM function: __%s__

        Selects only T1 images from a set of various DICOM images.

        Selected DICOM files are stored the the following locations:

        * Local folder: __%s__

        Depends on: __%s__
        """ % (dicom_select_t1_spm_function, dicom_select_t1_local_folder, upstream_id))

        upstream = dicom_select_t1_pipeline
        upstream_id = 'dicom_select_T1_pipeline'
        priority_weight += 5

    # endif

    extract_dicom_info = PythonPipelineOperator(
        task_id='extract_dicom_info',
        python_callable=extract_images_info_fn,
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

    # This upstream is required as we need to extract additional information from the DICOM files (participant_id,
    # scan_date) and transfer it via XCOMs to the next SPM pipeline
    upstream = extract_dicom_info
    upstream_id = 'extract_dicom_info'
    priority_weight += 5

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
        dataset_config=dataset_config,
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
    priority_weight += 5

    if copy_to_local:
        cleanup_local_dicom_cmd = dedent("""
            rm -rf {{ params["local_folder"] }}/{{ dag_run.conf["session_id"] }}
        """)

        cleanup_local_dicom = BashOperator(
            task_id='cleanup_local_dicom',
            bash_command=cleanup_local_dicom_cmd,
            params={'local_folder': copy_to_local_folder},
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=1),
            dag=dag
        )
        cleanup_local_dicom.set_upstream(upstream)
        priority_weight += 5

        cleanup_local_dicom.doc_md = dedent("""\
        # Cleanup local DICOM files

        Remove locally stored DICOM files as they have been processed already.
        """)

    extract_nifti_info = PythonPipelineOperator(
        task_id='extract_nifti_info',
        python_callable=extract_images_info_fn,
        parent_task=upstream_id,
        pool='io_intensive',
        priority_weight=priority_weight,
        execution_timeout=timedelta(hours=3),
        dag=dag
    )

    extract_nifti_info.set_upstream(dicom_to_nifti_pipeline)
    priority_weight += 5

    extract_nifti_info.doc_md = dedent("""\
    # Extract information from NIFTI files converted from DICOM

    Read NIFTI information from directory %s containing nifti files freshly converted from DICOM and store that
    information into the database.
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
            dataset_config=dataset_config,
            dag=dag
        )

        mpm_maps_pipeline.set_upstream(upstream)

        mpm_maps_pipeline.doc_md = dedent("""\
        # MPM Maps Pipeline

        SPM function: __%s__

        This function computes the Multiparametric Maps (MPMs) (R2*, R1, MT, PD) and brain segmentation in different
        tissue maps.
        All computation was programmed based on the LREN database structure.

        The MPMs are calculated locally and finally copied to a remote folder:

        * Local folder: __%s__
        * Remote folder: __%s__

        Depends on: __%s__
        """ % (mpm_maps_spm_function, mpm_maps_local_folder, mpm_maps_server_folder, upstream_id))

        upstream = mpm_maps_pipeline
        upstream_id = 'mpm_maps_pipeline'
        priority_weight += 5

        extract_nifti_mpm_info = PythonPipelineOperator(
            task_id='extract_nifti_mpm_info',
            python_callable=extract_images_info_fn,
            parent_task=upstream_id,
            pool='io_intensive',
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=3),
            dag=dag
        )

        extract_nifti_mpm_info.set_upstream(upstream)

        extract_nifti_mpm_info.doc_md = dedent("""\
        # Extract information from NIFTI files generated by MPM pipeline

        Read NIFTI information from directory %s containing the Nifti files created by MPM pipeline and store that
        information in the database.
        """ % mpm_maps_local_folder)

        notify_success.set_upstream(extract_nifti_mpm_info)
        priority_weight += 5

    # endif

    if neuro_morphometric_atlas:

        neuro_morphometric_atlas_pipeline = SpmPipelineOperator(
            task_id='neuro_morphometric_atlas_pipeline',
            spm_function=neuro_morphometric_atlas_spm_function,
            spm_arguments_callable=neuro_morphometric_atlas_arguments_fn,
            matlab_paths=[misc_library_path,
                          neuro_morphometric_atlas_pipeline_path,
                          mpm_maps_pipeline_path],
            output_folder_callable=lambda session_id, **kwargs: (neuro_morphometric_atlas_local_folder + '/' +
                                                                 session_id),
            pool='image_preprocessing',
            parent_task=upstream_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=24),
            on_skip_trigger_dag_id='mri_notify_skipped_processing',
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            dataset_config=dataset_config,
            dag=dag
        )
        neuro_morphometric_atlas_pipeline.set_upstream(upstream)

        neuro_morphometric_atlas_pipeline.doc_md = dedent("""\
        # NeuroMorphometric Pipeline

        SPM function: __%s__

        This function computes an individual Atlas based on the NeuroMorphometrics Atlas. This is based on the
        NeuroMorphometrics Toolbox.
        This delivers three files:

        1. Atlas File (*.nii);
        2. Volumes of the Morphometric Atlas structures (*.txt);
        3. CSV File (.csv) containing the volume, globals, and Multiparametric Maps (R2*, R1, MT, PD) for each
        structure defined in the Subject Atlas.

        The atlas is calculated locally and finally copied to a remote folder:

        * Local folder: %s
        * Remote folder: %s

        Depends on: __%s__
        """ % (neuro_morphometric_atlas_spm_function, neuro_morphometric_atlas_local_folder,
               neuro_morphometric_atlas_server_folder, upstream_id))

        upstream = neuro_morphometric_atlas_pipeline
        upstream_id = "neuro_morphometric_atlas_pipeline"
        priority_weight += 5

        extract_nifti_atlas_info = PythonPipelineOperator(
            task_id='extract_nifti_atlas_info',
            python_callable=extract_images_info_fn,
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

        Read NIFTI information from directory %s containing the Nifti files created by Neuro Morphometrics Atlas
        pipeline and store that information in the database.
        """ % neuro_morphometric_atlas_local_folder)

        notify_success.set_upstream(extract_nifti_atlas_info)
        priority_weight += 5

        import_features_pipeline = PythonPipelineOperator(
            task_id='import_features_pipeline',
            python_callable=features_to_i2b2_fn,
            output_folder_callable=lambda session_id, **kwargs: import_features_local_folder + '/' + session_id,
            pool='io_intensive',
            parent_task=upstream_id,
            priority_weight=priority_weight,
            execution_timeout=timedelta(hours=6),
            on_skip_trigger_dag_id='mri_notify_skipped_processing',
            on_failure_trigger_dag_id='mri_notify_failed_processing',
            dag=dag
        )

        import_features_pipeline.set_upstream(upstream)

        import_features_pipeline.doc_md = dedent("""\
        # import brain features from CSV files to I2B2 DB

        Read CSV files containing brain features and import it into an I2B2 DB.

        Depends on: __%s__
        """ % upstream_id)

    # endif

    return dag
