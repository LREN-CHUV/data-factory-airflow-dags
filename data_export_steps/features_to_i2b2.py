"""

  Data export step: features to I2B2

  Configuration variables used:

  * DATASET
  * DATASET_CONFIG
  * FEATURES_TO_I2B2_LOCAL_FOLDER

"""

import os

from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import PythonPipelineOperator

from common_steps import Step

from i2b2_import import features_csv_import


def features_to_i2b2_pipeline_cfg(dag, upstream_step, dataset_section):
    dataset = configuration.get(dataset_section, 'DATASET')
    dataset_config = configuration.get(dataset_section, 'DATASET_CONFIG')
    local_folder = configuration.get(dataset_section, 'FEATURES_TO_I2B2_LOCAL_FOLDER')

    return features_to_i2b2_pipeline(dag, upstream_step, local_folder, dataset, dataset_config)


def features_to_i2b2_pipeline(dag, upstream_step, local_folder=None, dataset='', dataset_config=None):

    def arguments_fn(folder, session_id, **kwargs):
        """
          Prepare the arguments.
        """
        parent_data_folder = os.path.abspath(folder + '/..')

        return [parent_data_folder,
                session_id,
                local_folder,
                dataset,
                dataset_config]

    def features_to_i2b2_fn(folder, **kwargs):
        """
          Import brain features from CSV files to I2B2 DB
        """
        features_csv_import.folder2db(folder, dataset, dataset_config)

        return "ok"

    features_to_i2b2_pipeline = PythonPipelineOperator(
        task_id='features_to_i2b2_pipeline',
        python_callable=features_to_i2b2_fn,
        kwargs=arguments_fn,
        output_folder_callable=lambda session_id, **kwargs: local_folder + '/' + session_id,
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
