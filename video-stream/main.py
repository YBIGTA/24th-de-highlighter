"""
Read stream, send from EC2 to SQS.
"""
import streamlink, datetime, sys, time, boto3
import base64

URL = sys.argv[1]
BUF_SIZE = int(sys.argv[2])
TIME = int(sys.argv[3])

streams = streamlink.streams(URL)
print("Streams found: ", *streams.keys())

fd = streams['best'].open()

now = time.time()
lines = 0
total_data = 0

# AWS SQS
sqs = boto3.client('sqs')
queue_url=''

data_id = 0
while time.time() - now < TIME:
    data = fd.read(BUF_SIZE)
    data_id += 1
    res = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=base64.b64encode(data).decode(),
            MessageGroupId="1",
            MessageDeduplicationId=str(data_id)
    )
    print(data_id, res['MessageId'], data[:8])