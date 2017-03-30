"""Initialise the DAGs for all the pipelines required to process different datasets containing MRI images"""

# Please keep keywords airflow and DAG in this file, otherwise the safe mode in DagBag may skip this file

import logging

from airflow import configuration
from common_steps import default_config

from preprocessing_pipelines.mri_notify_failed_processing import mri_notify_failed_processing_dag
from preprocessing_pipelines.mri_notify_skipped_processing import mri_notify_skipped_processing_dag
from preprocessing_pipelines.mri_notify_successful_processing import mri_notify_successful_processing_dag
from preprocessing_pipelines.continuously_pre_process_incoming import continuously_preprocess_incoming_dag
from preprocessing_pipelines.daily_pre_process_incoming import daily_preprocess_incoming_dag
from preprocessing_pipelines.flat_pre_process_incoming import flat_preprocess_incoming_dag
from preprocessing_pipelines.pre_process_images import pre_process_images_dag
from etl_pipelines.daily_ehr_incoming import daily_ehr_incoming_dag
from etl_pipelines.flat_ehr_incoming import flat_ehr_incoming_dag
from etl_pipelines.ehr_to_i2b2 import ehr_to_i2b2_dag
from reorganisation_pipelines.flat_reorganise import flat_reorganisation_dag
from reorganisation_pipelines.reorganise import reorganise_dag


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
    max_active_runs = int(configuration.get(reorganisation_section, 'MAX_ACTIVE_RUNS'))
    reorganisation_pipelines = configuration.get(reorganisation_section, 'PIPELINES').split(',')

    if reorganisation_pipelines and len(reorganisation_pipelines) > 0 and reorganisation_pipelines[0] != '':
        reorganisation_dag_id = register_dag(reorganise_dag(dataset=dataset,
                                                            section=reorganisation_section,
                                                            email_errors_to=email_errors_to,
                                                            max_active_runs=max_active_runs,
                                                            reorganisation_pipelines=reorganisation_pipelines))
        register_dag(flat_reorganisation_dag(
            dataset=dataset,
            folder=reorganisation_input_folder,
            depth=depth,
            email_errors_to=email_errors_to,
            trigger_dag_id=reorganisation_dag_id))
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
    pre_process_images_dag_id = register_dag(pre_process_images_dag(dataset=dataset,
                                                                    section=preprocessing_section,
                                                                    email_errors_to=email_errors_to,
                                                                    max_active_runs=max_active_runs,
                                                                    preprocessing_pipelines=preprocessing_pipelines))
    if 'continuous' in preprocessing_scanners:
        register_dag(continuously_preprocess_incoming_dag(
            dataset=dataset,
            folder=preprocessing_input_folder,
            email_errors_to=email_errors_to,
            trigger_dag_id=pre_process_images_dag_id))
    if 'daily' in preprocessing_scanners:
        register_dag(daily_preprocess_incoming_dag(
            dataset=dataset,
            folder=preprocessing_input_folder,
            email_errors_to=email_errors_to,
            trigger_dag_id=pre_process_images_dag_id))
    if 'flat' in preprocessing_scanners:
        register_dag(flat_preprocess_incoming_dag(
            dataset=dataset,
            folder=preprocessing_input_folder,
            email_errors_to=email_errors_to,
            trigger_dag_id=pre_process_images_dag_id))


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

        if 'daily' in ehr_scanners:
            register_dag(daily_ehr_incoming_dag(
                dataset=dataset, folder=ehr_input_folder, email_errors_to=email_errors_to,
                trigger_dag_id='%s_ehr_to_i2b2' % dataset.lower()))

        if 'flat' in ehr_scanners:
            ehr_input_folder_depth = int(configuration.get(ehr_section, 'INPUT_FOLDER_DEPTH'))
            register_dag(flat_ehr_incoming_dag(
                dataset=dataset, folder=ehr_input_folder, depth=ehr_input_folder_depth,
                email_errors_to=email_errors_to,
                trigger_dag_id='%s_ehr_to_i2b2' % dataset.lower()))
    register_dag(ehr_to_i2b2_dag(dataset=dataset, section=ehr_section,
                                 email_errors_to=email_errors_to,
                                 max_active_runs=max_active_runs))


def init_pipelines():
    default_config('mipmap', 'DB_CONFIG_FILE', '/dev/null')
    dataset_sections = configuration.get('data-factory', 'DATASETS')
    email_errors_to = configuration.get('data-factory', 'EMAIL_ERRORS_TO')
    register_dag(mri_notify_failed_processing_dag())
    register_dag(mri_notify_skipped_processing_dag())
    register_dag(mri_notify_successful_processing_dag())

    for dataset in dataset_sections.split(','):
        dataset_section = 'data-factory:%s' % dataset

        register_reorganisation_dags(dataset, dataset_section, email_errors_to)
        register_preprocessing_dags(dataset, dataset_section, email_errors_to)
        register_ehr_dags(dataset, dataset_section, email_errors_to)


init_pipelines()
