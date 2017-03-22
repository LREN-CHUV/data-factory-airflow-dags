"""

ETL step: Data Catalog to I2B2.

Import meta-data from the data catalog to the I2B2 database.

Configuration variables used:

  * data-factory section
    * DATA_CATALOG_DB_SQL_ALCHEMY_CONN
    * I2B2_DB_SQL_ALCHEMY_CONN

"""


from datetime import timedelta
from textwrap import dedent

from airflow import configuration
from airflow_pipeline.operators import PythonPipelineOperator

from common_steps import Step

from i2b2_import import data_catalog_import


def catalog_to_i2b2_pipeline_cfg(dag, upstream_step, data_factory_section):
    data_catalog_conn = configuration.get(data_factory_section, 'DATA_CATALOG_DB_SQL_ALCHEMY_CONN')
    i2b2_conn = configuration.get(data_factory_section, 'I2B2_DB_SQL_ALCHEMY_CONN')

    return catalog_to_i2b2_pipeline(dag, upstream_step, data_catalog_conn, i2b2_conn)


def catalog_to_i2b2_pipeline(dag, upstream_step, data_catalog_conn, i2b2_conn):

    def catalog_to_i2b2_fn(**kwargs):
        """
          Import meta-data from data catalog DB to I2B2 DB
        """
        data_catalog_import.catalog2i2b2(data_catalog_conn, i2b2_conn)

        return "ok"

    catalog_to_i2b2_pipeline = PythonPipelineOperator(
        task_id='catalog_to_i2b2_pipeline',
        python_callable=catalog_to_i2b2_fn,
        pool='io_intensive',
        execution_timeout=timedelta(hours=6),
        on_skip_trigger_dag_id='mri_notify_skipped_processing',
        on_failure_trigger_dag_id='mri_notify_failed_processing',
        dag=dag
    )

    if upstream_step.task:
        catalog_to_i2b2_pipeline.set_upstream(upstream_step.task)

        catalog_to_i2b2_pipeline.doc_md = dedent("""\
        # Data Catalog to I2B2 pipeline

        Import meta-data from the Data Catalog DB to the I2B2 database.

        Depends on: __%s__
        """ % upstream_step.task_id)

    return Step(catalog_to_i2b2_pipeline, 'catalog_to_i2b2_pipeline', upstream_step.priority_weight + 10)
