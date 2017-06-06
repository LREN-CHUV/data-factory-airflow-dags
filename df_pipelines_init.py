"""Initialise the DAGs for all the pipelines required to process different datasets containing MRI images"""

# Please keep keywords airflow and DAG in this file, otherwise the safe mode in DagBag may skip this file

import logging

from airflow import configuration
from common_steps import default_config

from preprocessing_pipelines.mri_notify_failed_processing import mri_notify_failed_processing_dag
from preprocessing_pipelines.mri_notify_skipped_processing import mri_notify_skipped_processing_dag
from preprocessing_pipelines.mri_notify_successful_processing import mri_notify_successful_processing_dag
from preprocessing_pipelines.pre_process_continuously_scan_input_folder \
    import pre_process_continuously_scan_input_folder_dag
from preprocessing_pipelines.pre_process_daily_scan_input_folder import pre_process_daily_scan_input_folder_dag
from preprocessing_pipelines.pre_process_scan_input_folder import pre_process_scan_input_folder_dag
from preprocessing_pipelines.pre_process_images import pre_process_images_dag
from ehr_pipelines.ehr_daily_scan_input_folder import ehr_daily_scan_input_folder_dag
from ehr_pipelines.ehr_scan_input_folder import ehr_scan_input_folder_dag
from ehr_pipelines.ehr_to_i2b2 import ehr_to_i2b2_dag
from reorganisation_pipelines.reorganisation_scan_input_folder import reorganisation_scan_input_folder_dag
from reorganisation_pipelines.reorganise_files import reorganise_files_dag
from metadata_pipelines.metadata_import import metadata_import_dag
from metadata_pipelines.metadata_scan_folder import metadata_scan_folder_dag


def register_dag(dag):
    dag_id = dag.dag_id
    var_name = "%s_dag" % dag_id
    globals()[var_name] = dag
    logging.info("Add DAG %s", dag_id)
    return dag_id


def register_reorganisation_dags(dataset, dataset_section, email_errors_to):
    reorganisation_section = dataset_section + ':reorganisation'
    default_config(reorganisation_section, 'INPUT_FOLDER_DEPTH', '1')

    reorganisation_input_folder = configuration.get(reorganisation_section, 'INPUT_FOLDER')
    depth = int(configuration.get(reorganisation_section, 'INPUT_FOLDER_DEPTH'))
    folder_filter = configuration.get(reorganisation_section, 'FOLDER_FILTER')
    max_active_runs = int(configuration.get(reorganisation_section, 'MAX_ACTIVE_RUNS'))
    reorganisation_pipelines = configuration.get(reorganisation_section, 'PIPELINES').split(',')

    if reorganisation_pipelines and len(reorganisation_pipelines) > 0 and reorganisation_pipelines[0] != '':
        reorganisation_dag_id = register_dag(reorganise_files_dag(dataset=dataset,
                                                                  section=reorganisation_section,
                                                                  email_errors_to=email_errors_to,
                                                                  max_active_runs=max_active_runs,
                                                                  reorganisation_pipelines=reorganisation_pipelines))
        register_dag(reorganisation_scan_input_folder_dag(
            dataset=dataset,
            folder=reorganisation_input_folder,
            depth=depth,
            email_errors_to=email_errors_to,
            trigger_dag_id=reorganisation_dag_id,
            folder_filter=folder_filter))
    # endif


def register_preprocessing_dags(dataset, dataset_section, email_errors_to):
    dataset_label = configuration.get(dataset_section, 'DATASET_LABEL')
    preprocessing_section = dataset_section + ':preprocessing'
    # Set the default configuration for the preprocessing of the dataset
    default_config(preprocessing_section, 'SCANNERS', 'daily')
    default_config(preprocessing_section, 'PIPELINES',
                   'copy_to_local,dicom_to_nifti,mpm_maps,neuro_morphometric_atlas')
    preprocessing_input_folder = configuration.get(
        preprocessing_section, 'INPUT_FOLDER')
    preprocessing_scanners = configuration.get(
        preprocessing_section, 'SCANNERS').split(',')
    preprocessing_pipelines = configuration.get(
        preprocessing_section, 'PIPELINES').split(',')
    max_active_runs = int(configuration.get(preprocessing_section, 'MAX_ACTIVE_RUNS'))
    logging.info("Create pipelines for dataset %s using scannners %s and pipelines %s",
                 dataset_label, preprocessing_scanners, preprocessing_pipelines)

    if preprocessing_pipelines and len(preprocessing_pipelines) > 0 and preprocessing_pipelines[0] != '':
        pre_process_images_dag_id = register_dag(
            pre_process_images_dag(dataset=dataset, section=preprocessing_section, email_errors_to=email_errors_to,
                                   max_active_runs=max_active_runs, preprocessing_pipelines=preprocessing_pipelines))
        if 'continuous' in preprocessing_scanners:
            register_dag(pre_process_continuously_scan_input_folder_dag(
                dataset=dataset,
                folder=preprocessing_input_folder,
                email_errors_to=email_errors_to,
                trigger_dag_id=pre_process_images_dag_id))
        if 'daily' in preprocessing_scanners:
            register_dag(pre_process_daily_scan_input_folder_dag(
                dataset=dataset,
                folder=preprocessing_input_folder,
                email_errors_to=email_errors_to,
                trigger_dag_id=pre_process_images_dag_id))
        if 'once' in preprocessing_scanners:
            register_dag(pre_process_scan_input_folder_dag(
                dataset=dataset,
                folder=preprocessing_input_folder,
                email_errors_to=email_errors_to,
                trigger_dag_id=pre_process_images_dag_id))
    # endif


def register_metadata_dags(dataset, dataset_section, email_errors_to):
    metadata_section = dataset_section + ':metadata'
    default_config(metadata_section, 'INPUT_FOLDER_DEPTH', '1')
    metadata_input_folder = configuration.get(metadata_section, 'INPUT_FOLDER')
    max_active_runs = int(configuration.get(metadata_section, 'MAX_ACTIVE_RUNS'))

    if metadata_input_folder != '':
        metadata_dag_id = register_dag(metadata_import_dag(dataset=dataset,
                                                           section='data-factory',
                                                           email_errors_to=email_errors_to,
                                                           max_active_runs=max_active_runs))
        register_dag(metadata_scan_folder_dag(
            dataset=dataset,
            folder=metadata_input_folder,
            email_errors_to=email_errors_to,
            trigger_dag_id=metadata_dag_id))


def register_ehr_dags(dataset, dataset_section, email_errors_to):
    ehr_section = dataset_section + ':ehr'
    # Set the default configuration for the preprocessing of the dataset
    default_config(ehr_section, 'SCANNERS', '')
    default_config(ehr_section, 'INPUT_FOLDER_DEPTH', '1')
    ehr_scanners = configuration.get(ehr_section, 'SCANNERS')
    max_active_runs = int(configuration.get(ehr_section, 'MAX_ACTIVE_RUNS'))
    if ehr_scanners != '':
        ehr_scanners = ehr_scanners.split(',')
        ehr_input_folder = configuration.get(ehr_section, 'INPUT_FOLDER')

        ehr_to_i2b2_dag_id = register_dag(ehr_to_i2b2_dag(dataset=dataset, section=ehr_section,
                                                          email_errors_to=email_errors_to,
                                                          max_active_runs=max_active_runs))
        if 'daily' in ehr_scanners:
            register_dag(ehr_daily_scan_input_folder_dag(
                dataset=dataset, folder=ehr_input_folder, email_errors_to=email_errors_to,
                trigger_dag_id=ehr_to_i2b2_dag_id))

        if 'once' in ehr_scanners:
            ehr_input_folder_depth = int(configuration.get(ehr_section, 'INPUT_FOLDER_DEPTH'))
            register_dag(ehr_scan_input_folder_dag(
                dataset=dataset, folder=ehr_input_folder, depth=ehr_input_folder_depth,
                email_errors_to=email_errors_to,
                trigger_dag_id=ehr_to_i2b2_dag_id))
    # endif


def init_pipelines():
    default_config('mipmap', 'DB_CONFIG_FILE', '/dev/null')
    dataset_sections = configuration.get('data-factory', 'DATASETS')
    email_errors_to = configuration.get('data-factory', 'EMAIL_ERRORS_TO')
    register_dag(mri_notify_failed_processing_dag())
    register_dag(mri_notify_skipped_processing_dag())
    register_dag(mri_notify_successful_processing_dag())

    for dataset in dataset_sections.split(','):
        dataset_section = 'data-factory:%s' % dataset

        if configuration.has_option(dataset_section + ':reorganisation', 'INPUT_FOLDER'):
            register_reorganisation_dags(dataset, dataset_section, email_errors_to)
        register_preprocessing_dags(dataset, dataset_section, email_errors_to)
        if configuration.has_option(dataset_section + ':metadata', 'INPUT_FOLDER'):
            register_metadata_dags(dataset, dataset_section, email_errors_to)
        if configuration.has_option(dataset_section + ':ehr', 'SCANNERS'):
            register_ehr_dags(dataset, dataset_section, email_errors_to)


init_pipelines()
