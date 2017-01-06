"""

Initialise the DAGs for all the pipelines required to process different datasets containing MRI images

"""

# Please keep keywords airflow and DAG in this file, otherwise the safe mode in DagBag may skip this file

import logging

from airflow import configuration
from mri_pipelines.continuously_pre_process_incoming import continuously_preprocess_incoming_dag
from mri_pipelines.daily_pre_process_incoming import daily_preprocess_incoming_dag
from mri_pipelines.flat_pre_process_incoming import flat_preprocess_incoming_dag
from mri_pipelines.pre_process_dicom import pre_process_dicom_dag


def default_config(section, key, value):
    if not configuration.has_option(section, key):
        configuration.set(section, key, value)

dataset_sections = configuration.get('mri', 'DATASETS')
email_errors_to = configuration.get('mri', 'EMAIL_ERRORS_TO')

for dataset_section in dataset_sections.split(','):
    # Set the default configuration for the dataset
    default_config(dataset_section, 'PREPROCESSING_SCANNERS', 'daily')
    default_config(dataset_section, 'PREPROCESSING_PIPELINES',
                   'dicom_to_nifti,mpm_maps,neuro_morphometric_atlas')
    default_config(dataset_section, 'COPY_DICOM_TO_LOCAL', 'True')
    default_config(dataset_section, 'DICOM_ORGANIZER_SPM_FUNCTION', 'dicomOrganizer')
    default_config(dataset_section, 'DICOM_ORGANIZER_LOCAL_FOLDER', '/dev/null')
    default_config(dataset_section, 'NIFTI_SPM_FUNCTION', 'DCM2NII_LREN')
    default_config(dataset_section, 'MPM_MAPS_SPM_FUNCTION',
                   'Preproc_mpm_maps')
    default_config(dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_SPM_FUNCTION',
                   'NeuroMorphometric_pipeline')

    preprocessing_scanners = configuration.get(
        dataset_section, 'PREPROCESSING_SCANNERS').split(',')
    dataset = configuration.get(dataset_section, 'DATASET')
    preprocessing_data_folder = configuration.get(
        dataset_section, 'PREPROCESSING_DATA_FOLDER')

    logging.info("Create pipelines for dataset %s" % dataset)

    if 'continuous' in preprocessing_scanners:
        dag = continuously_preprocess_incoming_dag(dataset=dataset, folder=preprocessing_data_folder,
                                             email_errors_to=email_errors_to, trigger_dag_id='%s_mri_pre_process_dicom' % dataset.lower())
    if 'daily' in preprocessing_scanners:
        dag = daily_preprocess_incoming_dag(dataset=dataset, folder=preprocessing_data_folder,
                                      email_errors_to=email_errors_to, trigger_dag_id='%s_mri_pre_process_dicom' % dataset.lower())
    if 'flat' in preprocessing_scanners:
        dag = flat_preprocess_incoming_dag(dataset=dataset, folder=preprocessing_data_folder,
                                     email_errors_to=email_errors_to, trigger_dag_id='%s_mri_pre_process_dicom' % dataset.lower())

    pipelines_path = configuration.get(dataset_section, 'PIPELINES_PATH')
    protocols_file = configuration.get(dataset_section, 'PROTOCOLS_FILE')
    max_active_runs = int(configuration.get(
        dataset_section, 'MAX_ACTIVE_RUNS'))
    preprocessing_pipelines = configuration.get(
        dataset_section, 'PREPROCESSING_PIPELINES').split(',')
    misc_library_path = pipelines_path + '/../Miscellaneous&Others'
    min_free_space_local_folder = configuration.getfloat(
        dataset_section, 'MIN_FREE_SPACE_LOCAL_FOLDER')
    dicom_local_folder = configuration.get(
        dataset_section, 'DICOM_LOCAL_FOLDER')
    copy_dicom_to_local = configuration.getboolean(
        dataset_section, 'COPY_DICOM_TO_LOCAL')
    dicom_organizer = 'dicom_organizer' in preprocessing_pipelines
    dicom_organizer_spm_function = configuration.get(
        dataset_section, 'DICOM_ORGANIZER_SPM_FUNCTION')
    dicom_organizer_local_folder = configuration.get(
        dataset_section, 'DICOM_ORGANIZER_LOCAL_FOLDER')
    dicom_organizer_data_structure = configuration.get(
        dataset_section, 'DICOM_ORGANIZER_DATA_STRUCTURE')
    dicom_organizer_pipeline_path = pipelines_path + '/DicomOrganizer_Pipeline'
    dicom_to_nifti_spm_function = configuration.get(
        dataset_section, 'NIFTI_SPM_FUNCTION')
    dicom_to_nifti_local_folder = configuration.get(
        dataset_section, 'NIFTI_LOCAL_FOLDER')
    dicom_to_nifti_server_folder = configuration.get(
        dataset_section, 'NIFTI_SERVER_FOLDER')
    dicom_to_nifti_pipeline_path = pipelines_path + '/Nifti_Conversion_Pipeline'
    mpm_maps = 'mpm_maps' in preprocessing_pipelines
    mpm_maps_spm_function = configuration.get(
        dataset_section, 'MPM_MAPS_SPM_FUNCTION')
    mpm_maps_local_folder = configuration.get(
        dataset_section, 'MPM_MAPS_LOCAL_FOLDER')
    mpm_maps_server_folder = configuration.get(
        dataset_section, 'MPM_MAPS_SERVER_FOLDER')
    mpm_maps_pipeline_path = pipelines_path + '/MPMs_Pipeline'
    neuro_morphometric_atlas = 'neuro_morphometric_atlas' in preprocessing_pipelines
    neuro_morphometric_atlas_spm_function = configuration.get(
        dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_SPM_FUNCTION')
    neuro_morphometric_atlas_local_folder = configuration.get(
        dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_LOCAL_FOLDER')
    neuro_morphometric_atlas_server_folder = configuration.get(
        dataset_section, 'NEURO_MORPHOMETRIC_ATLAS_SERVER_FOLDER')
    neuro_morphometric_atlas_pipeline_path = pipelines_path + \
        '/NeuroMorphometric_Pipeline/NeuroMorphometric_tbx/label'

    dag = pre_process_dicom_dag(dataset=dataset, email_errors_to=email_errors_to, max_active_runs=max_active_runs, misc_library_path=misc_library_path,
                          min_free_space_local_folder=min_free_space_local_folder, dicom_local_folder=dicom_local_folder,
                          copy_dicom_to_local=copy_dicom_to_local,
                          dicom_organizer=dicom_organizer, dicom_organizer_spm_function=dicom_organizer_spm_function, dicom_organizer_pipeline_path=dicom_organizer_pipeline_path,
                          dicom_organizer_local_folder=dicom_organizer_local_folder, dicom_organizer_data_structure=dicom_organizer_data_structure,
                          dicom_to_nifti_spm_function=dicom_to_nifti_spm_function, dicom_to_nifti_pipeline_path=dicom_to_nifti_pipeline_path,
                          dicom_to_nifti_local_folder=dicom_to_nifti_local_folder, dicom_to_nifti_server_folder=dicom_to_nifti_server_folder,
                          mpm_maps=mpm_maps, mpm_maps_spm_function=mpm_maps_spm_function, mpm_maps_pipeline_path=mpm_maps_pipeline_path,
                          mpm_maps_local_folder=mpm_maps_local_folder, mpm_maps_server_folder=mpm_maps_local_folder,
                          neuro_morphometric_atlas=neuro_morphometric_atlas, neuro_morphometric_atlas_spm_function=neuro_morphometric_atlas_spm_function,
                          neuro_morphometric_atlas_pipeline_path=neuro_morphometric_atlas_pipeline_path, neuro_morphometric_atlas_local_folder=neuro_morphometric_atlas_local_folder, neuro_morphometric_atlas_server_folder=neuro_morphometric_atlas_server_folder)
