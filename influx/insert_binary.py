import influxdb_client
from confluent_kafka import Consumer
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
import os
import base64
import itertools

load_dotenv()

# InfluxDB 환경 세팅
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url = os.getenv("INFLUXDB_URL")
bucket = os.getenv("INFLUXDB_BUCKET")

# Influx Client 생성
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

# Influx Write API 생성
write_api = client.write_api(write_options=SYNCHRONOUS)

# Kafka consumer configuration 세팅
# earliest -> 해당 파티션의 가장 오래된 메시지부터 소비
# latest -> 해당 파티션의 최신 메시지부터 소비
conf = {
    'bootstrap.servers': '52.79.81.77:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe(['mytopic'])

# 나노초 증가값을 생성하기 위한 카운터
nano_increment = itertools.count()

try:
    while True:
        msg = consumer.poll(1.0)  # 브로커로부터 메시지를 가져옴
        
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # 바이너리 메시지 및 타임스탬프 추출
        message_value = base64.b64encode(msg.value()).decode()
        kafka_timestamp = msg.timestamp()[1]
        
        influx_timestamp = (kafka_timestamp * 1_000_000) + next(nano_increment)

        # inlfuxDB에 넣기 위해 포인트 생성 
        point = Point("video_binary").field("value", message_value).time(influx_timestamp, WritePrecision.NS)

        # influxDB에 데이터 넣기
        write_api.write(bucket=bucket, org=org, record=point)

        print(kafka_timestamp)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
    client.close()