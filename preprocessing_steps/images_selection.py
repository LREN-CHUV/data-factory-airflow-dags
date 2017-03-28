"""

Pre processing step: images selection.

Configuration variables used:

* :preprocessing section
    * INPUT_CONFIG: List of flags defining how incoming imaging data are organised.
* :preprocessing:dicom_selection or :preprocessing:nifti_selection section
    * OUTPUT_FOLDER: destination folder for the selected images
    * IMAGES_SELECTION_CSV_PATH: path to the CSV file containing the list of selected images (PatientID | ImageID).

"""

# TODO: This whole step should be replaced by the standard image filtering done by the scanners

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import PythonPipelineOperator

from common_steps import Step, default_config


def images_selection_pipeline_cfg(dag, upstream_step, preprocessing_section, step_section):
    default_config(preprocessing_section, 'INPUT_CONFIG', '')

    dataset_config = configuration.get(preprocessing_section, 'INPUT_CONFIG')
    local_folder = configuration.get(step_section, 'OUTPUT_FOLDER')
    images_selection_csv_path = configuration.get(step_section, 'IMAGES_SELECTION_CSV_PATH')

    return images_selection_pipeline_step(dag, upstream_step, dataset_config, local_folder, images_selection_csv_path)


# TODO: add software_versions

def images_selection_pipeline_step(dag, upstream_step, dataset_config=None, local_folder=None, csv_path=None):

    def images_selection_fn(folder, **kwargs):
        """Selects files from DICOM/NIFTI that match criterion in CSV file.

        It selects all files located in the folder 'folder' matching criterion in CSV file
        """
        import csv
        from glob import iglob
        from os import makedirs
        from os import listdir
        from os.path import join
        from shutil import copy2

        with open(csv_path, mode='r', newline='') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',')
            for row in filereader:
                for folder in iglob(join(folder, row[0], "**/", row[1]), recursive=True):
                    path_elements = folder.split('/')
                    repetition_folder = join(local_folder, row[0], path_elements[-3],
                                             path_elements[-2], row[1])
                    makedirs(repetition_folder, exist_ok=True)
                    for file_ in listdir(folder):
                        copy2(join(folder, file_), join(repetition_folder, file_))
        return "ok"

    images_selection_pipeline = PythonPipelineOperator(
        task_id='images_selection_pipeline',
        python_callable=images_selection_fn,
        output_folder_callable=lambda session_id, **kwargs: local_folder + '/' + session_id,
        pool='io_intensive',
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=6),
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dataset_config=dataset_config,
        dag=dag
    )

    if upstream_step.task:
        images_selection_pipeline.set_upstream(upstream_step.task)

    images_selection_pipeline.doc_md = dedent("""\
        # select DICOM/NIFTI pipeline

        Selects only images matching criterion defined in a CSV file from a set of various DICOM/NIFTI images. For
        example we might want to keep only the baseline visits and T1 images.

        Selected DICOM/NIFTI files are stored the the following locations:

        * Target folder: __%s__

        Depends on: __%s__
        """ % (local_folder, upstream_step.task_id))

    return Step(images_selection_pipeline, images_selection_pipeline.task_id, upstream_step.priority_weight + 10)
