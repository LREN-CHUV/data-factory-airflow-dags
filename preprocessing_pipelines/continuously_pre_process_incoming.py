"""

Poll a base directory for incoming Dicom files ready for processing.

We assume that Dicom files are already processed by the hierarchize.sh script with the following directory structure:

  2016
     _ 20160407
        _ PR01471_CC082251
           _ .ready
           _ 1
              _ al_B1mapping_v2d
              _ gre_field_mapping_1acq_rl
              _ localizer

We are looking for the presence of the .ready marker file indicating that pre-processing of an MRI session is complete.

"""

from datetime import datetime, timedelta, time
from textwrap import dedent
from airflow import DAG
from airflow_scan_folder.operators import ScanDailyFolderOperator
from airflow_scan_folder.operators.common import extract_context_from_session_path, default_look_for_ready_marker_file
from airflow_scan_folder.operators.common import session_folder_trigger_dagrun
from airflow_scan_folder.operators.common import default_build_daily_folder_path_callable

from . import lren_accept_folder, lren_build_daily_folder_path_callable


def continuously_preprocess_incoming_dag(dataset, folder, email_errors_to, trigger_dag_id):
    # Param folder to scan for new incoming session folders containing DICOM images.

    start = datetime.utcnow()
    start = datetime.combine(start.date(), time(start.hour, 0))

    dag_name = '%s_mri_continuously_pre_process_incoming' % dataset.lower().replace(" ", "_")

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

    # Run the DAG every 10 minutes
    dag = DAG(dag_id=dag_name,
              default_args=default_args,
              schedule_interval='0 */10 * * *')

    accept_folder_fn = None
    build_daily_folder_path_callable = default_build_daily_folder_path_callable

    if dataset.lower() == 'lren':
        accept_folder_fn = lren_accept_folder
        build_daily_folder_path_callable = lren_build_daily_folder_path_callable

    scan_ready_dirs = ScanDailyFolderOperator(
        task_id='scan_dirs_ready_for_preprocessing',
        dataset=dataset,
        folder=folder,
        trigger_dag_id=trigger_dag_id,
        trigger_dag_run_callable=session_folder_trigger_dagrun,
        extract_context_callable=extract_context_from_session_path,
        accept_folder_callable=accept_folder_fn,
        build_daily_folder_path_callable=build_daily_folder_path_callable,
        look_for_ready_marker_file=default_look_for_ready_marker_file,
        execution_timeout=timedelta(minutes=10),
        dag=dag)

    scan_ready_dirs.doc_md = dedent("""\
    # Scan directories ready for processing

    Scan the session folders located inside folder %s (defined by variable __preprocessing_data_folder__) and organised
    by daily folders.

    It looks for the presence of a .ready marker file to mark that session folder as ready for processing, but it
    will skip it if contains the marker file .processing indicating that processing has already started.
    """ % folder)

    return dag
