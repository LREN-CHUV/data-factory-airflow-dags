"""

Take EHR data located in a study folder and convert it to I2B2.

Poll a base directory for incoming CSV files ready for processing. We assume that
CSV files are already anonymised and organised with the following directory structure:

  2016
     _ 20160407
        _ patients.csv
        _ diseases.csv
        _ ...

"""


from datetime import datetime, timedelta

from airflow import DAG

from common_steps import initial_step
from common_steps.check_local_free_space import check_local_free_space_cfg
from common_steps.prepare_pipeline import prepare_pipeline

from etl_steps.map_ehr_to_i2b2 import map_ehr_to_i2b2_pipeline_cfg
from etl_steps.version_incoming_ehr import version_incoming_ehr_pipeline_cfg


steps_with_file_outputs = ['version_incoming_ehr']


def ehr_to_i2b2_dag(dataset, section, email_errors_to, max_active_runs):

    # Define the DAG

    dag_name = '%s_ehr_to_i2b2' % dataset.lower().replace(" ", "_")

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.now(),
        'retries': 1,
        'retry_delay': timedelta(seconds=120),
        'email': email_errors_to,
        'email_on_failure': True,
        'email_on_retry': True
    }

    dag = DAG(
        dag_id=dag_name,
        default_args=default_args,
        schedule_interval=None,
        max_active_runs=max_active_runs)

    upstream_step = check_local_free_space_cfg(dag, initial_step, section,
                                               map(lambda p: section + ':' + p, steps_with_file_outputs))

    upstream_step = prepare_pipeline(dag, upstream_step, False)

    upstream_step = version_incoming_ehr_pipeline_cfg(dag, upstream_step, section, section + ':version_incoming_ehr')

    # TODO Next: Python to build provenance_details

    # Call MipMap on versioned folder
    map_ehr_to_i2b2_pipeline_cfg(dag, upstream_step, section, section + ':map_ehr_to_i2b2')

    # TODO Call MipMap to convert original data in I2B2 format to the MIP CDE (Common Data Elements)
    # also in I2B2 format but stored in another database
    # map_i2b2_to_mip_i2b2_pipeline_cfg(dag, upstream_step, section, section + ':map_i2b2_to_mip_i2b2')

    return dag
