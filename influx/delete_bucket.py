from influxdb_client import InfluxDBClient
from dotenv import load_dotenv
import os

load_dotenv()

# InfluxDB 환경 설정
token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url = os.getenv("INFLUXDB_URL")

# InfluxDB 클라이언트 생성
client = InfluxDBClient(url=url, token=token, org=org)

# Bucket API 사용
buckets_api = client.buckets_api()

# 삭제할 버킷 이름
bucket_to_delete = "삭제할 버킷 이름"

# 버킷 찾기
bucket = buckets_api.find_bucket_by_name(bucket_to_delete)

if bucket:
    # 버킷 삭제
    buckets_api.delete_bucket(bucket)
    print(f"Bucket '{bucket_to_delete}' has been deleted.")
else:
    print(f"Bucket '{bucket_to_delete}' not found.")