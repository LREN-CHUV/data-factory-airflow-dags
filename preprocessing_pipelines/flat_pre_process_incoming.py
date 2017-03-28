"""

Poll a base directory for incoming Dicom files ready for processing.

We assume that Dicom files are already processed by the hierarchize.sh script with the following directory structure:

  <base dir>
    _ <scan session id>
       _ <repetition>
          _ al_B1mapping_v2d
          _ gre_field_mapping_1acq_rl
          _ localizer

"""

from datetime import datetime, timedelta, time
from textwrap import dedent
from airflow import DAG
from airflow_scan_folder.operators import ScanFolderOperator


def flat_preprocess_incoming_dag(dataset, folder, email_errors_to, trigger_dag_id):
    # Folder to scan for new incoming session folders containing DICOM images.

    start = datetime.utcnow()
    start = datetime.combine(start.date(), time(start.hour, 0))

    dag_name = '%s_mri_flat_pre_process_incoming' % dataset.lower().replace(" ", "_")

    # Define the DAG

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': start,
        'retries': 1,
        'retry_delay': timedelta(seconds=120),
        'email': email_errors_to,
        'email_on_failure': True,
        'email_on_retry': True
    }

    dag = DAG(dag_id=dag_name,
              default_args=default_args,
              schedule_interval='@once')

    scan_dirs = ScanFolderOperator(
        task_id='scan_dirs',
        folder=folder,
        trigger_dag_id=trigger_dag_id,
        dataset=dataset,
        execution_timeout=timedelta(minutes=30),
        dag=dag)

    scan_dirs.doc_md = dedent("""\
    # Scan directories for processing

    Scan the session folders located inside folder %s (defined by variable __preprocessing_data_folder__).
    """ % folder)

    return dag
