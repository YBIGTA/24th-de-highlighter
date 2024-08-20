from influxdb_client import InfluxDBClient
import os
from dotenv import load_dotenv

load_dotenv()

# InfluxDB 환경 변수 로드
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url = os.getenv("INFLUXDB_URL")
bucket = "HighLighter"  # 데이터를 삭제할 버킷 이름

client = InfluxDBClient(url=url, token=token, org=org)
delete_api = client.delete_api()

# 삭제할 measurement 이름
measurement_to_delete = "삭제할 measurement 이름"

# 모든 데이터를 삭제하는 시간 범위 설정 (전체 범위)
start = "1970-01-01T00:00:00Z"
stop = "2100-01-01T00:00:00Z"

# Measurement 데이터 삭제
delete_api.delete(start, stop, f'_measurement="{measurement_to_delete}"', bucket=bucket, org=org)

print(f"Measurement '{measurement_to_delete}' has been deleted.")