"""

  Pre processing step: extract DICOM info

  Configuration variables used:

  * DATASET_CONFIG

"""


import logging

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import PythonPipelineOperator

from mri_meta_extract.files_recording import create_provenance, visit


def extract_images_info_fn(folder, session_id, step_name, dataset, dataset_config, software_versions=None, **kwargs):
    """
     Extract the information from DICOM/NIFTI files located inside a folder.
     The folder information should be given in the configuration parameter
     'folder' of the DAG run
    """
    logging.info('Extracting information from files in folder %s, session_id %s', folder, session_id)

    provenance = create_provenance(dataset, software_versions=software_versions)
    step = visit(folder, provenance, step_name, config=dataset_config)

    return step


def extract_dicom_info_cfg(dag, upstream, upstream_id, priority_weight, dataset, dataset_section):
    dataset_config = configuration.get(dataset_section, 'DATASET_CONFIG')

    return extract_dicom_info(dag, upstream, upstream_id, priority_weight, dataset, dataset_config)


def extract_dicom_info(dag, upstream, upstream_id, priority_weight,
                       dataset, dataset_config):

    extract_dicom_info = PythonPipelineOperator(
        task_id='extract_dicom_info',
        python_callable=extract_images_info_fn,
        op_kwargs={'dataset_config': dataset_config, 'dataset': dataset},
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
    priority_weight += 10

    return (upstream, upstream_id, priority_weight)
