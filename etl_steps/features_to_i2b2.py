"""

Data export step: features to I2B2.

Exports neuroimaging features stored in CSV files to the I2B2 database.

Configuration variables used:

* :ehr section
    * INPUT_CONFIG
* :ehr:export_features section
    * INPUT_FOLDER

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import PythonPipelineOperator

from common_steps import Step

from i2b2_import import features_csv_import


def features_to_i2b2_pipeline_cfg(dag, upstream_step, etl_section, section):
    dataset_config = configuration.get(etl_section, 'INPUT_CONFIG')
    input_folder = configuration.get(section, 'INPUT_FOLDER')

    return features_to_i2b2_pipeline(dag, upstream_step, input_folder, dataset_config)


def features_to_i2b2_pipeline(dag, upstream_step, input_folder=None, dataset_config=None):

    def features_to_i2b2_fn(dataset, **kwargs):
        """
          Import brain features from CSV files to I2B2 DB
        """
        features_csv_import.folder2db(input_folder, dataset, dataset_config)

        return "ok"

    features_to_i2b2_pipeline = PythonPipelineOperator(
        task_id='features_to_i2b2_pipeline',
        python_callable=features_to_i2b2_fn,
        pool='io_intensive',
        parent_task=upstream_step.task_id,
        priority_weight=upstream_step.priority_weight,
        execution_timeout=timedelta(hours=6),
        on_skip_trigger_dag_id='mri_notify_skipped_processing',
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dag=dag
    )

    if upstream_step.task:
        features_to_i2b2_pipeline.set_upstream(upstream_step.task)

    features_to_i2b2_pipeline.doc_md = dedent("""\
        # Features to I2B2 pipeline

        Exports brain features (from CSV file) to I2B2 database.

        Depends on: __%s__
        """ % upstream_step.task_id)

    return Step(features_to_i2b2_pipeline, 'images_selection_pipeline', upstream_step.priority_weight + 10)
