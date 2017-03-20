"""Inform administrators when processing on a MRI session failed"""

from datetime import datetime, timedelta
from airflow import DAG, configuration
from airflow.operators import SlackAPIPostOperator


def mri_notify_failed_processing_dag():

    # constants

    DAG_NAME = 'mri_notify_failed_processing'

    slack_token = str(configuration.get('data-factory', 'SLACK_TOKEN'))
    slack_channel = str(configuration.get('data-factory', 'SLACK_CHANNEL'))
    slack_channel_user = str(configuration.get('data-factory', 'SLACK_CHANNEL_USER'))

    # Define the DAG

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.now(),
        'retries': 1,
        'retry_delay': timedelta(seconds=120),
        'email': None,
        'email_on_failure': False,
        'email_on_retry': False
    }

    dag = DAG(
        dag_id=DAG_NAME,
        default_args=default_args,
        schedule_interval=None)

    post_on_slack = SlackAPIPostOperator(
        task_id='post_on_slack',
        token=slack_token,
        channel=slack_channel,
        username=slack_channel_user,
        text='@channel :boom: *{{ dag_run.conf["dataset"] }}*: '
             + '`Failed processing for scan session *{{ dag_run.conf["session_id"] }}*'
             + '{% if dag_run.conf["task_id"] %} at stage {{ dag_run.conf["task_id"] }}{% endif %}`\n'
             + '> Scan {% if dag_run.conf["scan_date"] %}'
             + 'done on {{ dag_run.conf["scan_date"].strftime("%Y-%m-%d") }} {% endif %}'
             + 'for participant {{ dag_run.conf["participant_id"] | default("?") }}\n'
             + '> Output:\n'
             + '> ```\n\n{{ dag_run.conf["spm_output"] | default("?") }}\n\n```\n'
             + '> Errors:\n'
             + '> ```\n\n{{ dag_run.conf["spm_error"] | default("?") }}\n\n```',
        icon_url='https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png',
        dag=dag
    )

    post_on_slack.doc_md = """\
    # Post information about the failed MRI scan session on Slack

    Post information about the failed MRI scan session on Slack channel %s
    """ % slack_channel

    return dag
