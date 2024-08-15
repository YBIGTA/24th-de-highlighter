"""
Save video to S3, triggered by SQS.
"""
import json
import uuid
import boto3
import subprocess
import time
import base64
import shlex
from datetime import datetime


def lambda_handler(event, context):
    
    s3 = boto3.client("s3")
    sqs = boto3.client('sqs')
    response = sqs.get_queue_url(QueueName='highlighter.fifo')
    queue_url = response["QueueUrl"]
    
    bucket_name = "de-highlighter"
    file_name = str(uuid.uuid4())

    # Parse timestamp from query parameters
    offset = 60 * 60 * 9
    start_str = event["queryStringParameters"]["start"]
    start_time = time.mktime(
        datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S").timetuple()) - offset
    
    end_str = event["queryStringParameters"]["end"]
    end_time = time.mktime(
        datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S").timetuple()) - offset
    
    msg = event["Records"][0]
    body = json.loads(msg["body"])
    start_time = body["start"]
    end_time = body["end"]
        
    with open(f"/tmp/vid.ts", "wb") as video_file:
        while True:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                AttributeNames=[
                    "SentTimestamp"
                ],
                MaxNumberOfMessages=1,
                MessageAttributeNames=[
                    "All"
                ],
                VisibilityTimeout=100,
                WaitTimeSeconds=0 # Short polling
            )
    
            # Non-empty response
            if "Messages" in response:
                message = response["Messages"][0]
                receipt_handle = message["ReceiptHandle"]
    
                sent_timestamp = message['Attributes']['SentTimestamp']
    
                # Delete received message from queue
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )
    
                # if time.time() - float(sent_timestamp) / 1000 > 30:
                if float(sent_timestamp) / 1000 > end_time:
                    break
                elif float(sent_timestamp) / 1000 < start_time:
                    pass
                else:
                    body = base64.b64decode(message["Body"].encode())
                    video_file.write(body)
            else:
                # TODO: Check emptiness of SQS
                break
    
    try:
        ffmpeg_cmd = f"/opt/ffmpeg/ffmpeg -i /tmp/vid.ts -c copy /tmp/{file_name}.mp4"
        command1 = shlex.split(ffmpeg_cmd)
        p1 = subprocess.run(command1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s3.upload_file(Filename=f"/tmp/{file_name}.mp4", Bucket=bucket_name, Key=f"{file_name}.mp4")
        return {
            'statusCode': 200,
            'sent': json.dumps(float(sent_timestamp) / 1000),
            'start': json.dumps(start_time),
            'end': json.dumps(end_time),
            'body': json.dumps("Upload success")
        }
    except Exception as e:
        s3.put_object(Body=f"Error: {e}", Bucket=bucket_name, Key=f"error-{file_name}.txt")
        return {
            'statusCode': 400,
            'sent': json.dumps(float(sent_timestamp) / 1000),
            'start': json.dumps(start_time),
            'end': json.dumps(end_time),
            'body': json.dumps(f"Fail: {e}")
        }
        
