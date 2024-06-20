from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

class SlackNotification:
    def __init__(self, slack_conn_id):
        self.slack_conn_id = slack_conn_id
        self.slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
        self.channel = BaseHook.get_connection(slack_conn_id).login

    def send_alert(self, context):
        ti = context['ti']
        task_state = ti.state

        if task_state == 'success':
            slack_msg = f"""
            :white_check_mark: Task Succeeded.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            """
        elif task_state == 'failed':
            slack_msg = f"""
            :x: Task Failed.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            <{context.get('task_instance').log_url}|*Logs*>
            """

        slack_alert = SlackWebhookOperator(
            task_id='slack_alert',
            webhook_token=self.slack_webhook_token,
            message=slack_msg,
            channel=self.channel,
            username='airflow',
            http_conn_id=self.slack_conn_id
        )

        return slack_alert.execute(context=context)
