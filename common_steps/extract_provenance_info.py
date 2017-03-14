import logging

from mri_meta_extract.files_recording import create_provenance, visit


def extract_provenance_info_fn(folder, session_id, step_name, dataset, dataset_config,
                               software_versions=None, **kwargs):
    """
     Extract the provenance information from DICOM/NIFTI files located inside a folder.
     The folder information should be given in the configuration parameter
     'folder' of the DAG run
    """
    logging.info('Extracting information from files in folder %s, session_id %s', folder, session_id)

    provenance = create_provenance(dataset, software_versions=software_versions)
    step = visit(folder, provenance, step_name, config=dataset_config)

    return step
