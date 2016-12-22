"""

Inform administrators when processing on a MRI session failed

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import SlackAPIPostOperator
from airflow import configuration

# constants

DAG_NAME = 'mri_notify_failed_processing'

slack_token = str(configuration.get('mri', 'SLACK_TOKEN'))
slack_channel = str(configuration.get('mri', 'SLACK_CHANNEL'))
slack_channel_user = str(configuration.get('mri', 'SLACK_CHANNEL_USER'))

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
    text=':boom: *{{ dag_run.conf["dataset"] }}*: Failed processing for scan session *{{ dag_run.conf["session_id"] }}*\nScan {% if dag_run.conf["scan_date"] %}done on {{ dag_run.conf["scan_date"].strftime("%Y-%m-%d") }} {% endif %}for participant {{ dag_run.conf["participant_id"] | default("?") }}{% if dag_run.conf["task_id"] %} at stage {{ dag_run.conf["task_id"] }}{% endif %}\n\n'
    + 'Output:\n{{ dag_run.conf["spm_output"] | default("?") }}\n\nErrors:\n{{ dag_run.conf["spm_error"] | default("?") }}',
    icon_url='https://raw.githubusercontent.com/airbnb/airflow/master/airflow/www/static/pin_100.png',
    dag=dag
)

post_on_slack.doc_md = """\
# Post information about the failed MRI scan session on Slack

Post information about the failed MRI scan session on Slack channel %s
""" % slack_channel
