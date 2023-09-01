import json
import requests
import os

def lambda_handler(event, context):
    # Slack 웹훅 URL 환경 변수에서 가져오기
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("Slack Webhook URL not found")
        return {
            'statusCode': 400,
            'body': json.dumps('Slack Webhook URL not found')
        }

    # 이벤트에서 필요한 정보 추출
    bucket_name = event['detail']['bucket']['name']
    object_key = event['detail']['object']['key']
    detail_type = event.get('detail-type', '')

    # Slack 메시지 초기 설정
    slack_message = {"text": ""}

    # detail-type 별로 메시지 내용 변경
    if detail_type == "Object Created":
        object_size = event['detail']['object']['size']
        slack_message["text"] = "A new object has been created in S3"
        slack_message["attachments"] = [
            {
                "text": f"Bucket Name: {bucket_name}\nObject Key: {object_key}\nObject Size: {object_size} bytes"
            }
        ]
    elif detail_type == "Object Deleted":
        slack_message["text"] = "An object has been deleted from S3"
        slack_message["attachments"] = [
            {
                "text": f"Bucket Name: {bucket_name}\nObject Key: {object_key}"
            }
        ]
    else:
        slack_message["text"] = "Unknown S3 operation"

    # Slack 웹훅을 통해 메시지 전송
    response = requests.post(
        webhook_url, data=json.dumps(slack_message),
        headers={'Content-Type': 'application/json'}
    )

    if response.status_code != 200:
        print(f"Failed to send slack message. Status Code: {response.status_code}, Reason: {response.text}")

    return {
        'statusCode': 200,
        'body': json.dumps('Slack message sent')
    }
