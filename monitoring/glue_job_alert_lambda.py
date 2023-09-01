from datetime import datetime
import json
import os
import requests
import boto3


def lambda_handler(event, context):
    webhook_url = os.environ.get('SLACK_WEBHOOK_URL')

    if not webhook_url:
        print('Slack Webhook URL not found')
        return {
            'statusCode': 400,
            'body': json.dumps('Slack Webhook URL not found')
        }

    glue_job_name = event['detail']['jobName']
    glue_job_run_id = event['detail']['jobRunId']
    glue_job_state = event['detail']['state']

    slack_message = {}
    message_text = ''
    attachments = ''
    if glue_job_state == 'SUCCEEDED':
        # message_text = f'Glue Job {glue_job_name} has succeeded!'
        glue = boto3.client('glue')
        response = glue.get_job_run(JobName=glue_job_name, RunId=glue_job_run_id)

        execution_time = response['JobRun']['ExecutionTime']
        hours, remainder = divmod(execution_time, 3600)
        minutes, seconds = divmod(remainder, 60)

        message_text = f"Glue Job {glue_job_name} has succeeded!"
        attachments = f"Execution Time: {hours}h {minutes}m {seconds}s."

    elif glue_job_state == 'FAILED':
        message_text = f'Glue Job {glue_job_name} has failed.'
    elif glue_job_state == 'TIMEOUT':
        message_text = f'Glue Job {glue_job_name} has timed out.'
    elif glue_job_state == 'STOPPED':
        message_text = f'Glue Job {glue_job_name} has been stopped.'

    slack_message['text'] = message_text
    if attachments:
        slack_message["attachments"] = [
            {
                "text": attachments
            }
        ]

    response = requests.post(webhook_url, json=slack_message)

    if response.status_code != 200:
        print(f'Failed to send Slack message. Status Code: {response.status_code}, Reason: {response.text}')

    return {
        'statusCode': 200,
        'body': json.dumps('Slack message sent')
    }
