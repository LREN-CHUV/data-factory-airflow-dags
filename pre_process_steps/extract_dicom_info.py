"""

  Pre processing step: extract DICOM info

  Configuration variables used:

  * DATASET_CONFIG

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import PythonPipelineOperator

from common_steps.extract_provenance_info import extract_provenance_info_fn


def extract_dicom_info_cfg(dag, upstream, upstream_id, priority_weight, dataset, dataset_section):
    dataset_config = configuration.get(dataset_section, 'DATASET_CONFIG')

    return extract_dicom_info(dag, upstream, upstream_id, priority_weight, dataset, dataset_config)


def extract_dicom_info(dag, upstream, upstream_id, priority_weight,
                       dataset, dataset_config):

    extract_dicom_info = PythonPipelineOperator(
        task_id='extract_dicom_info',
        python_callable=extract_provenance_info_fn,
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

    return upstream, upstream_id, priority_weight
