[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/LREN-CHUV/data-factory-airflow-dags/blob/master/LICENSE) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/8c5c9dc3cfb8492f870369c973f3cc8c)](https://www.codacy.com/app/hbp-mip/data-factory-airflow-dags?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=LREN-CHUV/data-factory-airflow-dags&amp;utm_campaign=Badge_Grade) [![Code Health](https://landscape.io/github/LREN-CHUV/data-factory-airflow-dags/master/landscape.svg?style=flat)](https://landscape.io/github/LREN-CHUV/data-factory-airflow-dags/master)

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


* In Airflow config file, add a [data-factory] section with the following entries:
   * DATASETS: comma separated list of datasets to process. Each dataset is configured using a [&lt;dataset&gt;] section in the config file
   * EMAIL_ERRORS_TO: email address to send errors to
   * SLACK_CHANNEL: optional, Slack channel to use to send status messages
   * SLACK_CHANNEL_USER: optional, user to post as in Slack
   * SLACK_TOKEN: optional, authorisation token for Slack
   * SQL_ALCHEMY_CONN: connection URL to the Data catalog database tracking artifacts generated by the MRI pipelines.
   * MIPMAP_DB_CONFILE_FILE: optional, path to the configuration file used by MipMap to connect to its work database.


* For each dataset, add a [data-factory:&lt;dataset&gt;] section, replacing &lt;dataset&gt; with the name of the dataset and define the following entries:
   * DATASET_LABEL: Name of the dataset


* For each dataset, now configure the [data-factory:&lt;dataset&gt;:preprocessing] section:
   * INPUT_FOLDER: Folder containing the original imaging data to process. This data should have been already anonymised by a tool
   * INPUT_CONFIG: List of flags defining how incoming imaging data is organised, values are
      * boost: (optional) When enabled, we consider that all the files from a same folder share the same meta-data. The processing is (about 2 times) faster. This option is enabled by default.
      * session_id_by_patient: Rarely, a data set might use study IDs which are unique by patient (not for the whole study). E.g.: LREN data. In such a case, you have to enable this flag. This will use PatientID + StudyID as a session ID.
      * visit_id_in_patient_id: Rarely, a data set might mix patient IDs and visit IDs. E.g. : LREN data. In such a case, you have to enable this flag. This will try to split PatientID into VisitID and PatientID.
      * visit_id_from_path: Enable this flag to get the visit ID from the folder hierarchy instead of DICOM meta-data (e.g. can be useful for PPMI).
      * repetition_from_path: Enable this flag to get the repetition ID from the folder hierarchy instead of DICOM meta-data (e.g. can be useful for PPMI).
   * MAX_ACTIVE_RUNS: maximum number of folders containing scans to pre-process in parallel
   * MIN_FREE_SPACE: minimum percentage of free space available on local disk
   * MISC_LIBRARY_PATH: path to the Misc&Libraries folder for SPM pipelines.
   * PIPELINES_PATH: path to the root folder containing the Matlab scripts for the pipelines
   * SCANNERS: List of methods describing how the preprocessing data folder is scanned for new work, values are
      * continuous: input folder is scanned frequently for new data. Sub-folders should contain a .ready file to indicate that processing can be performed on that folder.
      * daily: input folder contains a sub-folder for the year containing daily sub-folders for each day of the year (format yyyyMMdd). Those daily sub-folders contain the folders for each scan to process.
      * flat: input folder contains a set of sub-folders each containing a scan to process.
   * PIPELINES: List of pipelines to execute. Values are
      * copy_to_local: if used, input data is first copied to a local folder to speed-up processing.
      * dicom_organiser: reorganise DICOM files in a scan folder for the following pipelines.
      * dicom_selection
      * dicom_to_nifti
      * mpm_maps
      * neuro_morphometric_atlas


* If copy_to_local is used, configure the [data-factory:&lt;dataset&gt;:preprocessing:copy_to_local] section:
   * OUTPUT_FOLDER: destination folder for the local copy

* If dicom_organiser is used, configure the [data-factory:&lt;dataset&gt;:preprocessing:dicom_organiser] section:
   * OUTPUT_FOLDER: destination folder for the organised images
   * DATA_STRUCTURE: folder hierarchy (e.g. 'PatientID:AcquisitionDate:SeriesDescription:SeriesDate')
   * DOCKER_IMAGE: Docker image of the hierarchizer program
   * DOCKER_INPUT_DIR: Input directory inside the Docker container. Default to '/input_folder'
   * DOCKER_OUTPUT_DIR: Output directory inside the Docker container. Default to '/output_folder'

* If dicom_selection is used, configure the [data-factory:&lt;dataset&gt;:preprocessing:dicom_selection] section:
   * OUTPUT_FOLDER: destination folder for the selected images
   * IMAGES_SELECTION_CSV_PATH: path to the CSV file containing the list of selected images (PatientID | ImageID).

* If dicom_select_T1 is used, configure the [data-factory:&lt;dataset&gt;:preprocessing:dicom_select_T1] section:
   * OUTPUT_FOLDER: destination folder for the selected T1 images
   * SPM_FUNCTION: SPM function called. Default to 'selectT1'
   * PROTOCOLS_FILE
   * PIPELINE_PATH
   * MISC_LIBRARY_PATH: path to the Misc&Libraries folder for SPM pipelines. Default to MISC_LIBRARY_PATH value in [data-factory:&lt;dataset&gt;:preprocessing] section.

   * EHR_SCANNERS
   * EHR_DATA_FOLDER
   * EHR_DATA_FOLDER_DEPTH
   * EHR_TO_I2B2_CAPTURE_DOCKER_IMAGE
   * EHR_TO_I2B2_CAPTURE_FOLDER
   * EHR_VERSIONED_FOLDER
   * IMAGES_ORGANIZER_DATASET_TYPE: image type (e.g. DICOM, NIFTI)
   * IMAGES_ORGANIZER_DATA_STRUCTURE: folder hierarchy (e.g. 'PatientID:AcquisitionDate:SeriesDescription:SeriesDate')
   * IMAGES_ORGANIZER_OUTPUT_FOLDER: output folder
   * IMAGES_ORGANIZER_DOCKER_IMAGE: organizer image
   * MPM_MAPS_OUTPUT_FOLDER: path for the results of MPM maps pipeline
   * MPM_MAPS_SERVER_FOLDER: for the results of MPM maps pipeline
   * MPM_MAPS_SPM_FUNCTION: Preproc_mpm_maps
   * neuro_morphometric_atlas_TPM_template = /opt/spm12/tpm/nwTPM_sl3.nii
   * NEURO_MORPHOMETRIC_ATLAS_OUTPUT_FOLDER: path for the results of neuro morphometric atlas pipeline
   * NEURO_MORPHOMETRIC_ATLAS_SERVER_FOLDER: long term storage location for the results of neuro morphometric atlas pipeline
   * NEURO_MORPHOMETRIC_ATLAS_SPM_FUNCTION: NeuroMorphometric_pipeline
   * NEURO_MORPHOMETRIC_ATLAS_TPM_TEMPLATE: SPM_DIR + '/tpm/nwTPM_sl3.nii'
   * NIFTI_OUTPUT_FOLDER: path for the image files converted to Nifti
   * NIFTI_SERVER_FOLDER: long term storage location for the image files converted to Nifti
   * NIFTI_SPM_FUNCTION: DCM2NII_LREN'
   * PIPELINES_PATH: path to the root folder containing the Matlab scripts for the pipelines
   * copy_to_local: copies all DICOM files to COPY_TO_OUTPUT_FOLDER.
   * PROTOCOLS_FILE: path to the MRI acquisition protocol file

Sample configuration:

```

[data-factory]
datasets = main
email_errors_to =
mipmap_db_confile_file = /dev/null
slack_channel = #data
slack_channel_user = Airflow
slack_token =
sql_alchemy_conn = postgresql://data_catalog:datacatalogpwd@demo:4321/data_catalog
[data-factory:main]
dataset = Demo
dataset_config = boost
dicom_files_pattern = **/*.dcm
dicom_local_folder = /data/incoming
dicom_select_T1_local_folder = /data/select_T1
dicom_select_T1_protocols_file = /opt/airflow-scripts/mri-preprocessing-pipeline/Protocols_definition.txt
ehr_data_folder = /data/ehr_demo
ehr_data_folder_depth = 0
ehr_scanners = flat
ehr_to_i2b2_capture_docker_image = hbpmip/mipmap-demo-ehr-to-i2b2:0.1
ehr_to_i2b2_capture_folder = /data/ehr_i2b2_capture
ehr_versioned_folder = /data/ehr_versioned
images_organizer_data_structure = PatientID:AcquisitionDate:SeriesDescription:SeriesDate
images_organizer_dataset_type = DICOM
images_organizer_docker_image = hbpmip/hierarchizer:latest
images_organizer_local_folder = /data/organizer
images_selection_csv_path = /data/incoming/images_selection.csv
images_selection_local_folder = /data/images_selection
max_active_runs = 30
min_free_space_local_folder = 0.3
mpm_maps_local_folder = /data/mpm_maps
mpm_maps_server_folder =
neuro_morphometric_atlas_TPM_template = /opt/spm12/tpm/nwTPM_sl3.nii
neuro_morphometric_atlas_local_folder = /data/neuro_morphometric_atlas
neuro_morphometric_atlas_server_folder =
nifti_local_folder = /data/nifti
nifti_server_folder =
pipelines_path = /opt/airflow-scripts/mri-preprocessing-pipeline/Pipelines
preprocessing_data_folder = /data/demo
preprocessing_pipelines = dicom_organizer,images_selection,dicom_select_T1,dicom_to_nifti,mpm_maps,neuro_morphometric_atlas
preprocessing_scanners = flat
protocols_file = /opt/airflow-scripts/mri-preprocessing-pipeline/Protocols_definition.txt

```
