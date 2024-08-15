"""
Enqueue request to SQS, triggered by API Gateway.
"""
import json
import boto3
import time
import uuid
from datetime import datetime


def lambda_handler(event, context):
    sqs = boto3.client('sqs')
    res = sqs.get_queue_url(QueueName='highlighter-requests.fifo')
    queue_url = res["QueueUrl"]
    
    # Parse timestamp from query parameters
    offset = 60 * 60 * 9
    start_str = event["queryStringParameters"]["start"]
    start_time = time.mktime(
        datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S").timetuple()) - offset
    
    end_str = event["queryStringParameters"]["end"]
    end_time = time.mktime(
        datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S").timetuple()) - offset
    
    try:
        res = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps({
                "start": start_time,
                "end": end_time,
            }),
            MessageGroupId="1",
            MessageDeduplicationId=str(uuid.uuid4())
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps("Request successfully enqueued")
        }
        
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps(f"Request failure: {e}")
        }
    