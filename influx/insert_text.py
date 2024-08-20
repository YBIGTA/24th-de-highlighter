import influxdb_client
import os
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
load_dotenv()

# InfluxDB 환경 세팅
token = os.environ.get("INFLUXDB_TOKEN")
org = os.environ.get("INFLUXDB_ORG")
url = os.environ.get("INFLUXDB_URL")
bucket = os.environ.get("INFLUXDB_BUCKET")

# Influx Client 생성
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

# Influx Write API 생성
write_api = client.write_api(write_options=SYNCHRONOUS)

#----------------------------------------------------------------------
#----------------------------------------------------------------------
# 임의의 변수
text_data = "model_output" #예시
timestamp = "2024-08-18 00:00:04" #예시, utc기준, 시작시간을 저장
# 데이터 넣기
# -------------------------------------------------------------------
# .time에는 카프카에서 받은 시간 ()
point = (
    Point("video_text")
    .field("text", text_data)
    .time(timestamp, WritePrecision.S)
)
#----------------------------------------------------------------------
#----------------------------------------------------------------------

# 데이터 입력
write_api.write(bucket=bucket, org=org, record=point)

client.close()