"""

Initialise the DAGs for all the pipelines required to process different datasets containing MRI images

"""

# Please keep keywords airflow and DAG in this file, otherwise the safe mode in DagBag may skip this file

import logging
import os

from airflow import configuration
from mri_pipelines.continuously_pre_process_incoming import continuously_preprocess_incoming_dag
from mri_pipelines.daily_pre_process_incoming import daily_preprocess_incoming_dag
from mri_pipelines.flat_pre_process_incoming import flat_preprocess_incoming_dag
from mri_pipelines.pre_process_dicom import pre_process_dicom_dag
from mri_pipelines.daily_ehr_incoming import daily_ehr_incoming_dag
from mri_pipelines.ehr_to_i2b2 import ehr_to_i2b2_dag


def default_config(section, key, value):
    if not configuration.has_option(section, key):
        configuration.set(section, key, value)

default_config('mri', 'MIPMAP_DB_CONFILE_FILE', '/dev/null')

dataset_sections = configuration.get('mri', 'DATASETS')
email_errors_to = configuration.get('mri', 'EMAIL_ERRORS_TO')
mipmap_db_confile_file = configuration.get('mri', 'MIPMAP_DB_CONFILE_FILE')

for dataset_section in dataset_sections.split(','):
    # Set the default configuration for the dataset
    default_config(dataset_section, 'SESSION_ID_BY_PATIENT', 'False')
    default_config(dataset_section, 'PREPROCESSING_SCANNERS', 'daily')
    default_config(dataset_section, 'PREPROCESSING_PIPELINES',
                   'dicom_to_nifti,mpm_maps,neuro_morphometric_atlas')
    default_config(dataset_section, 'DICOM_COPY_TO_LOCAL', 'True')
    default_config(dataset_section, 'DICOM_ORGANIZER_SPM_FUNCTION', 'dicomOrganizer')
    default_config(dataset_section, 'DICOM_SELECT_T1_SPM_FUNCTION', 'selectT1')
    default_config(dataset_section, 'DICOM_FILES_PATTERN', '**/MR.*')
    default_config(dataset_section, 'NIFTI_SPM_FUNCTION', 'DCM2NII_LREN')
    default_config(dataset_section, 'MPM_MAPS_SPM_FUNCTION',
                   'Preproc_mpm_maps')
    default_config(dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_SPM_FUNCTION',
                   'NeuroMorphometric_pipeline')
    default_config(dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_TPM_TEMPLATE',
                   configuration.get('spm', 'SPM_DIR') + '/tpm/nwTPM_sl3.nii')
    default_config(dataset_section, 'EHR_SCANNERS', '')

    dataset = configuration.get(dataset_section, 'DATASET')
    preprocessing_data_folder = configuration.get(
        dataset_section, 'PREPROCESSING_DATA_FOLDER')
    preprocessing_scanners = configuration.get(
        dataset_section, 'PREPROCESSING_SCANNERS').split(',')
    preprocessing_pipelines = configuration.get(
        dataset_section, 'PREPROCESSING_PIPELINES').split(',')

    logging.info("Create pipelines for dataset %s using scannners %s and pipelines %s",
                 dataset, preprocessing_scanners, preprocessing_pipelines)

    if 'continuous' in preprocessing_scanners:
        name = '%s_continuous_prepro_dag' % dataset.lower().replace(" ", "_")
        globals()[name] = continuously_preprocess_incoming_dag(dataset=dataset, folder=preprocessing_data_folder,
                                                               email_errors_to=email_errors_to, trigger_dag_id='%s_mri_pre_process_dicom' % dataset.lower())
        logging.info("Add DAG %s", globals()[name].dag_id)
    if 'daily' in preprocessing_scanners:
        name = '%s_daily_prepro_dag' % dataset.lower().replace(" ", "_")
        globals()[name] = daily_preprocess_incoming_dag(dataset=dataset, folder=preprocessing_data_folder,
                                                        email_errors_to=email_errors_to, trigger_dag_id='%s_mri_pre_process_dicom' % dataset.lower())
        logging.info("Add DAG %s", globals()[name].dag_id)
    if 'flat' in preprocessing_scanners:
        name = '%s_flat_prepro_dag' % dataset.lower().replace(" ", "_")
        globals()[name] = flat_preprocess_incoming_dag(dataset=dataset, folder=preprocessing_data_folder,
                                                       email_errors_to=email_errors_to, trigger_dag_id='%s_mri_pre_process_dicom' % dataset.lower())
        logging.info("Add DAG %s", globals()[name].dag_id)

    pipelines_path = configuration.get(dataset_section, 'PIPELINES_PATH')
    protocols_file = configuration.get(dataset_section, 'PROTOCOLS_FILE')

    default_config(dataset_section, 'DCM2NII_PROGRAM', pipelines_path + '/Nifti_Conversion_Pipeline/dcm2nii')

    max_active_runs = int(configuration.get(
        dataset_section, 'MAX_ACTIVE_RUNS'))
    session_id_by_patient = bool(configuration.get(
        dataset_section, 'SESSION_ID_BY_PATIENT'))
    misc_library_path = pipelines_path + '/../Miscellaneous&Others'
    min_free_space_local_folder = configuration.getfloat(
        dataset_section, 'MIN_FREE_SPACE_LOCAL_FOLDER')
    dicom_local_folder = configuration.get(
        dataset_section, 'DICOM_LOCAL_FOLDER')
    dicom_files_pattern = configuration.get(
        dataset_section, 'DICOM_FILES_PATTERN')
    dicom_copy_to_local = configuration.getboolean(
        dataset_section, 'DICOM_COPY_TO_LOCAL')
    dicom_to_nifti_spm_function = configuration.get(
        dataset_section, 'NIFTI_SPM_FUNCTION')
    dicom_to_nifti_local_folder = configuration.get(
        dataset_section, 'NIFTI_LOCAL_FOLDER')
    dicom_to_nifti_server_folder = configuration.get(
        dataset_section, 'NIFTI_SERVER_FOLDER')
    dicom_to_nifti_pipeline_path = pipelines_path + '/Nifti_Conversion_Pipeline'
    dcm2nii_program = configuration.get(
        dataset_section, 'DCM2NII_PROGRAM')
    dicom_organizer = 'dicom_organizer' in preprocessing_pipelines
    dicom_select_T1 = 'dicom_select_T1' in preprocessing_pipelines
    mpm_maps = 'mpm_maps' in preprocessing_pipelines
    neuro_morphometric_atlas = 'neuro_morphometric_atlas' in preprocessing_pipelines

    params = dict(dataset=dataset, email_errors_to=email_errors_to, max_active_runs=max_active_runs, session_id_by_patient=session_id_by_patient, misc_library_path=misc_library_path,
                  min_free_space_local_folder=min_free_space_local_folder, dicom_local_folder=dicom_local_folder, dicom_files_pattern=dicom_files_pattern,
                  dicom_copy_to_local=dicom_copy_to_local, dicom_organizer=dicom_organizer, dicom_select_T1=dicom_select_T1, protocols_file=protocols_file,
                  dicom_to_nifti_spm_function=dicom_to_nifti_spm_function, dicom_to_nifti_pipeline_path=dicom_to_nifti_pipeline_path,
                  dicom_to_nifti_local_folder=dicom_to_nifti_local_folder, dicom_to_nifti_server_folder=dicom_to_nifti_server_folder,
                  mpm_maps=mpm_maps, neuro_morphometric_atlas=neuro_morphometric_atlas, dcm2nii_program=dcm2nii_program)

    if dicom_organizer:
        params['dicom_organizer_spm_function'] = configuration.get(dataset_section, 'DICOM_ORGANIZER_SPM_FUNCTION')
        params['dicom_organizer_local_folder'] = configuration.get(dataset_section, 'DICOM_ORGANIZER_LOCAL_FOLDER')
        params['dicom_organizer_data_structure'] = configuration.get(dataset_section, 'DICOM_ORGANIZER_DATA_STRUCTURE')
        params['dicom_organizer_pipeline_path'] = pipelines_path + '/DicomOrganizer_Pipeline'

    if dicom_select_T1:
        params['dicom_select_T1_spm_function'] = configuration.get(dataset_section, 'DICOM_SELECT_T1_SPM_FUNCTION')
        params['dicom_select_T1_local_folder'] = configuration.get(dataset_section, 'DICOM_SELECT_T1_LOCAL_FOLDER')
        params['dicom_select_T1_protocols_file'] = configuration.get(dataset_section, 'DICOM_SELECT_T1_PROTOCOLS_FILE')
        params['dicom_select_T1_pipeline_path'] = pipelines_path + '/SelectT1_Pipeline'

    if mpm_maps:
        params['mpm_maps_spm_function'] = configuration.get(dataset_section, 'MPM_MAPS_SPM_FUNCTION')
        params['mpm_maps_local_folder'] = configuration.get(dataset_section, 'MPM_MAPS_LOCAL_FOLDER')
        params['mpm_maps_server_folder'] = configuration.get(dataset_section, 'MPM_MAPS_SERVER_FOLDER')

        params['mpm_maps_pipeline_path'] = pipelines_path + '/MPMs_Pipeline'

    if neuro_morphometric_atlas:
        params['neuro_morphometric_atlas_spm_function'] = configuration.get(
            dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_SPM_FUNCTION')
        params['neuro_morphometric_atlas_local_folder'] = configuration.get(
            dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_LOCAL_FOLDER')
        params['neuro_morphometric_atlas_server_folder'] = configuration.get(
            dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_SERVER_FOLDER')
        params['neuro_morphometric_atlas_pipeline_path'] = pipelines_path + \
            '/NeuroMorphometric_Pipeline/NeuroMorphometric_tbx/label'
        params['mpm_maps_pipeline_path'] = pipelines_path + '/MPMs_Pipeline'
        params['neuro_morphometric_atlas_TPM_template'] = tpmTemplate = configuration.get(dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_TPM_TEMPLATE')
        # check that file exists if absolute path
        if len(tpmTemplate) > 0 and tpmTemplate[0] is '/':
            if not os.path.isfile(tpmTemplate):
                raise OSError("TPM template file %s does not exist" % tpmTemplate)

    name = '%s_preprocess_dag' % dataset.lower().replace(" ", "_")
    globals()[name] = pre_process_dicom_dag(**params)
    logging.info("Add DAG %s", globals()[name].dag_id)

    ehr_scanners = configuration.get(dataset_section, 'EHR_SCANNERS')

    if ehr_scanners != '':
        ehr_scanners = ehr_scanners.split(',')
        ehr_data_folder = configuration.get(dataset_section, 'EHR_DATA_FOLDER')
        ehr_versioned_folder = configuration.get(dataset_section, 'EHR_VERSIONED_FOLDER')
        ehr_to_i2b2_capture_docker_image = configuration.get(dataset_section, 'EHR_TO_I2B2_CAPTURE_DOCKER_IMAGE')
        ehr_to_i2b2_capture_folder = configuration.get(dataset_section, 'EHR_TO_I2B2_CAPTURE_FOLDER')

        if 'daily' in ehr_scanners:
            name = '%s_daily_ehr_dag' % dataset.lower().replace(" ", "_")
            globals()[name] = daily_ehr_incoming_dag(dataset=dataset, folder=ehr_data_folder,
                                                            email_errors_to=email_errors_to, trigger_dag_id='%s_ehr_to_i2b2' % dataset.lower())
            logging.info("Add DAG %s", globals()[name].dag_id)

        params = dict(dataset=dataset, email_errors_to=email_errors_to, max_active_runs=max_active_runs,
                      min_free_space_local_folder=min_free_space_local_folder,
                      mipmap_db_confile_file=mipmap_db_confile_file,
                      ehr_versioned_folder=ehr_versioned_folder,
                      ehr_to_i2b2_capture_docker_image=ehr_to_i2b2_capture_docker_image, ehr_to_i2b2_capture_folder=ehr_to_i2b2_capture_folder)

        name = '%s_ehr_to_i2b2_dag' % dataset.lower().replace(" ", "_")
        globals()[name] = ehr_to_i2b2_dag(**params)
        logging.info("Add DAG %s", globals()[name].dag_id)
