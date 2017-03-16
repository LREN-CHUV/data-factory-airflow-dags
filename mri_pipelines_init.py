""" Initialise the DAGs for all the pipelines required to process different datasets containing MRI images """

# Please keep keywords airflow and DAG in this file, otherwise the safe mode in DagBag may skip this file

import logging

from airflow import configuration
from common_steps import default_config

from preprocessing_pipelines.continuously_pre_process_incoming import continuously_preprocess_incoming_dag
from preprocessing_pipelines.daily_pre_process_incoming import daily_preprocess_incoming_dag
from preprocessing_pipelines.flat_pre_process_incoming import flat_preprocess_incoming_dag
from preprocessing_pipelines.pre_process_dicom import pre_process_dicom_dag
from etl_pipelines.daily_ehr_incoming import daily_ehr_incoming_dag
from etl_pipelines.flat_ehr_incoming import flat_ehr_incoming_dag
from etl_pipelines.ehr_to_i2b2 import ehr_to_i2b2_dag


default_config('mri', 'MIPMAP_DB_CONFILE_FILE', '/dev/null')

dataset_sections = configuration.get('mri', 'DATASETS')
email_errors_to = configuration.get('mri', 'EMAIL_ERRORS_TO')
mipmap_db_confile_file = configuration.get('mri', 'MIPMAP_DB_CONFILE_FILE')

for dataset_section in dataset_sections.split(','):
    # Set the default configuration for the dataset
    default_config(dataset_section, 'DATASET_CONFIG', '')
    default_config(dataset_section, 'PREPROCESSING_SCANNERS', 'daily')
    default_config(dataset_section, 'PREPROCESSING_PIPELINES',
                   'copy_to_local,dicom_to_nifti,mpm_maps,neuro_morphometric_atlas')
    default_config(dataset_section, 'EHR_SCANNERS', '')
    default_config(dataset_section, 'EHR_DATA_FOLDER_DEPTH', '1')

    dataset = configuration.get(dataset_section, 'DATASET')
    preprocessing_data_folder = configuration.get(
        dataset_section, 'PREPROCESSING_DATA_FOLDER')
    preprocessing_scanners = configuration.get(
        dataset_section, 'PREPROCESSING_SCANNERS').split(',')
    preprocessing_pipelines = configuration.get(
        dataset_section, 'PREPROCESSING_PIPELINES').split(',')
    max_active_runs = int(configuration.get(dataset_section, 'MAX_ACTIVE_RUNS'))

    logging.info("Create pipelines for dataset %s using scannners %s and pipelines %s",
                 dataset, preprocessing_scanners, preprocessing_pipelines)

    if 'continuous' in preprocessing_scanners:
        name = '%s_continuous_prepro_dag' % dataset.lower().replace(" ", "_")
        globals()[name] = continuously_preprocess_incoming_dag(
            dataset=dataset, folder=preprocessing_data_folder, email_errors_to=email_errors_to,
            trigger_dag_id='%s_mri_pre_process_dicom' % dataset.lower())
        logging.info("Add DAG %s", globals()[name].dag_id)
    if 'daily' in preprocessing_scanners:
        name = '%s_daily_prepro_dag' % dataset.lower().replace(" ", "_")
        globals()[name] = daily_preprocess_incoming_dag(
            dataset=dataset, folder=preprocessing_data_folder, email_errors_to=email_errors_to,
            trigger_dag_id='%s_mri_pre_process_dicom' % dataset.lower())
        logging.info("Add DAG %s", globals()[name].dag_id)
    if 'flat' in preprocessing_scanners:
        name = '%s_flat_prepro_dag' % dataset.lower().replace(" ", "_")
        globals()[name] = flat_preprocess_incoming_dag(
            dataset=dataset, folder=preprocessing_data_folder, email_errors_to=email_errors_to,
            trigger_dag_id='%s_mri_pre_process_dicom' % dataset.lower())
        logging.info("Add DAG %s", globals()[name].dag_id)

    name = '%s_preprocess_dag' % dataset.lower().replace(" ", "_")
    globals()[name] = pre_process_dicom_dag(dataset=dataset, dataset_section=dataset_section,
                                            email_errors_to=email_errors_to, max_active_runs=max_active_runs,
                                            preprocessing_pipelines=preprocessing_pipelines)
    logging.info("Add DAG %s", globals()[name].dag_id)

    ehr_scanners = configuration.get(dataset_section, 'EHR_SCANNERS')

    ehr_versioned_folder = None
    ehr_to_i2b2_capture_docker_image = None
    ehr_to_i2b2_capture_folder = None

    if ehr_scanners != '':
        ehr_scanners = ehr_scanners.split(',')
        ehr_data_folder = configuration.get(dataset_section, 'EHR_DATA_FOLDER')
        ehr_versioned_folder = configuration.get(dataset_section, 'EHR_VERSIONED_FOLDER')
        ehr_to_i2b2_capture_docker_image = configuration.get(dataset_section, 'EHR_TO_I2B2_CAPTURE_DOCKER_IMAGE')
        ehr_to_i2b2_capture_folder = configuration.get(dataset_section, 'EHR_TO_I2B2_CAPTURE_FOLDER')

        if 'daily' in ehr_scanners:
            name = '%s_daily_ehr_dag' % dataset.lower().replace(" ", "_")
            globals()[name] = daily_ehr_incoming_dag(
                dataset=dataset, folder=ehr_data_folder, email_errors_to=email_errors_to,
                trigger_dag_id='%s_ehr_to_i2b2' % dataset.lower())
            logging.info("Add DAG %s", globals()[name].dag_id)

        if 'flat' in ehr_scanners:
            ehr_data_folder_depth = int(configuration.get(dataset_section, 'EHR_DATA_FOLDER_DEPTH'))
            name = '%s_flat_ehr_dag' % dataset.lower().replace(" ", "_")
            globals()[name] = flat_ehr_incoming_dag(
                dataset=dataset, folder=ehr_data_folder, depth=ehr_data_folder_depth, email_errors_to=email_errors_to,
                trigger_dag_id='%s_ehr_to_i2b2' % dataset.lower())
            logging.info("Add DAG %s", globals()[name].dag_id)

    min_free_space_local_folder = configuration.getfloat(
        dataset_section, 'MIN_FREE_SPACE_LOCAL_FOLDER')

    params = dict(dataset=dataset, email_errors_to=email_errors_to, max_active_runs=max_active_runs,
                  min_free_space_local_folder=min_free_space_local_folder,
                  mipmap_db_confile_file=mipmap_db_confile_file,
                  ehr_versioned_folder=ehr_versioned_folder,
                  ehr_to_i2b2_capture_docker_image=ehr_to_i2b2_capture_docker_image,
                  ehr_to_i2b2_capture_folder=ehr_to_i2b2_capture_folder)

    name = '%s_ehr_to_i2b2_dag' % dataset.lower().replace(" ", "_")
    globals()[name] = ehr_to_i2b2_dag(**params)
    logging.info("Add DAG %s", globals()[name].dag_id)
