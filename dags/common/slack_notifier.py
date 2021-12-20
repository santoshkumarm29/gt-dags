import uuid
import requests
import json
import os
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError


def send_slack_notification(slack_api_key, slackchannel, event_name, message, band_color, watchers):
    try:
        body = {
            "eventId": str(uuid.uuid4()),
            "eventType": "airflow/notifications",
            "eventTime": datetime.utcnow().isoformat() + "Z",
            "eventName": event_name,
            "eventLink": "",
            "action": "create",
            "color": band_color,
            "text":  message,
            "env": "prod",
            "pretext": "",
            "appId": "CODA",
            "eventUpdates": [],
            "creatorId": "rganesan",
            "watchersIds": watchers,
            "slackChannel": slackchannel
        }
        headers = {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
            'Authorization': slack_api_key
        }
        response = requests.post(
            "https://1azkn1b90a.execute-api.us-east-1.amazonaws.com/dprod/v1/events",
            data=json.dumps(body),
            headers=headers
        )
        print("SlackNS response: {} - message: {}".format(response.status_code, response.text))
        return response
    except Exception as e:
        logging.error("Unable to send slack notification: {e}".format(e=e))


def send_sns_slack_notification(slackchannel, event_name, message, band_color, watchers,dag_name,topic_arn,task_name = "", event_id = str(uuid.uuid4())):
    try:
        message = {
         "eventId": event_id,
          "eventType": "airflow/notifications",
          "eventTime": datetime.utcnow().isoformat() + "Z",
          "eventName": event_name,
          "eventLink": "",
          "color": band_color,
          "action": "create",
          "pretext": "",
          "text": message,
          "env": "prod",
          "details": "",
          "appId": dag_name,
          "eventUpdates": [
            {
              "task_name": task_name,
            }
          ],
          "creatorId": "airflow",
          "watchersIds": watchers,
          "slackChannel": slackchannel
        }
        client = boto3.client('sns')
        response = client.publish(TopicArn = topic_arn,Message=json.dumps(message))
        message_id = response['MessageId']

        print("Published message %s to topic %s.", message_id, topic_arn)
        return message_id
    except ClientError as e:
        print("Couldn't publish message to topic %s.", topic_arn)
        print(e)