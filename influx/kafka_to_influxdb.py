from confluent_kafka import Consumer
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import os
import base64


load_dotenv()

# InfluxDB connection details
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url = os.getenv("INFLUXDB_URL")
bucket = os.getenv("INFLUXDB_BUCKET")

# Create InfluxDB client
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Kafka consumer configuration
conf = {
    'bootstrap.servers': '52.79.81.77:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['mytopic'])

try:
    while True:
        msg = consumer.poll()  # Poll for a message
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Assuming the message is in a string format. Adapt this based on your message format.
        message_value = base64.b64encode(msg.value()).decode()

        # Create InfluxDB data point
        point = influxdb_client.Point("heng1").field("value", message_value)
        
        # Write the data point to InfluxDB
        write_api.write(bucket=bucket, org=org, record=point)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    client.close()


