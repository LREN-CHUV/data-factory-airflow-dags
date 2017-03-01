[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/LREN-CHUV/airflow-mri-preprocessing-dags/blob/master/LICENSE) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/8c5c9dc3cfb8492f870369c973f3cc8c)](https://www.codacy.com/app/hbp-mip/airflow-mri-preprocessing-dags?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=LREN-CHUV/airflow-mri-preprocessing-dags&amp;utm_campaign=Badge_Grade) [![Code Health](https://landscape.io/github/LREN-CHUV/airflow-mri-preprocessing-dags/master/landscape.svg?style=flat)](https://landscape.io/LREN-CHUV/airflow-mri-preprocessing-dags/master)

# Airflow MRI preprocessing DAGs

Requirements:

* airflow-imaging-plugins
* mri-preprocessing-pipeline
* mri-meta-extract

## Setup and configuration

### Airflow setup for MRI scans pipeline:

* Create the following pools:
   * image_preprocessing with N slots, where N is less than the number of vCPUs available on the machine
   * remote_file_copy with N slots, where N should be 1 or 2 to avoid saturating network IO

TODO: next section needs update

* In Airflow config file, add the [mri] section with the following entries:
   * PIPELINES_PATH: path to the root folder containing the Matlab scripts for the pipelines
   * PROTOCOLS_FILE: path to the MRI acquisition protocol file
   * MIN_FREE_SPACE_LOCAL_FOLDER: minimum percentage of free space available on local disk
   * DICOM_LOCAL_FOLDER: path containing the anonymised DICOM files coming from the MRI scanner and already anonymised by a tool
   * NIFTI_LOCAL_FOLDER: path for the image files converted to Nifti
   * NIFTI_SERVER_FOLDER: long term storage location for the image files converted to Nifti
   * NEURO_MORPHOMETRIC_ATLAS_LOCAL_FOLDER: path for the results of neuro morphometric atlas pipeline
   * NEURO_MORPHOMETRIC_ATLAS_SERVER_FOLDER: long term storage location for the results of neuro morphometric atlas pipeline
   * MPM_MAPS_LOCAL_FOLDER: path for the results of MPM maps pipeline
   * MPM_MAPS_SERVER_FOLDER: for the results of MPM maps pipeline
   * PREPROCESSING_DATA_FOLDER:
   * PREPROCESSING_SCANNERS: comma-separated list of values in continuous, daily, flat to select how the preprocessing data folder is scanned for new work.
   * PREPROCESSING_PIPELINES: comma-separated list of values in dicom_to_nifti, mpm_maps, neuro_morphometric_atlas
   * COPY_DICOM_TO_LOCAL: boolean
   * DICOM_ORGANIZER_SPM_FUNCTION: dicomOrganizer
   * DICOM_SELECT_T1_SPM_FUNCTION: selectT1
   * DICOM_FILES_PATTERN: ```**/MR.*```
   * NIFTI_SPM_FUNCTION: DCM2NII_LREN'
   * MPM_MAPS_SPM_FUNCTION: Preproc_mpm_maps
   * NEURO_MORPHOMETRIC_ATLAS_SPM_FUNCTION: NeuroMorphometric_pipeline
   * NEURO_MORPHOMETRIC_ATLAS_TPM_TEMPLATE: SPM_DIR + '/tpm/nwTPM_sl3.nii'
   * SESSION_ID_BY_PATIENT
   * EHR_SCANNERS
