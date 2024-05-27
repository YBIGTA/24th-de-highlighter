import json
import uuid
import boto3
import subprocess
import time
import base64
import shlex


def lambda_handler(event, context):
    
    s3 = boto3.client("s3")
    sqs = boto3.client('sqs')
    response = sqs.get_queue_url(QueueName='highlighter.fifo')
    queue_url = response["QueueUrl"]
    
    bucket_name = "de-highlighter"
    file_name = str(uuid.uuid4())
    
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
    
                # TODO: Replace time.time() to given value from Kafka
                # trigger_time = ...
                if time.time() - float(sent_timestamp) / 1000 > 30:
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
            'body': json.dumps("Upload success")
        }
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps(f"Fail: {e}")
        }
    

    
