import influxdb_client
import os
from dotenv import load_dotenv
import csv

load_dotenv()

# influxDB 세팅
influxdb_token = os.getenv('INFLUXDB_TOKEN')
influxdb_url = os.getenv('INFLUXDB_URL')
influxdb_org = os.getenv('INFLUXDB_ORG')
bucket = os.getenv('INFLUXDB_BUCKET')
csv.field_size_limit(1048576)

# 클라이언트 생성
client = influxdb_client.InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org)

# Read API 생성
query_api = client.query_api()

# 첫 데이터부터 (1970년 00-00), 현재 시점까지읟 데이터를 읽을 거임
start_time = "0"  # Start time
end_time = "now()"  # Current time

# influxdb 읽기 (전체 데이터)
# 다른 measurement에 있는 데이터를 읽을 시에, video_text 부분을 바꿔줘야됨
# 타임스탬프 필터링 및 정렬을 추가한 쿼리
query = f'''
from(bucket: "{bucket}")
  |> range(start: {start_time})
  |> filter(fn: (r) => r["_measurement"] == "video_text")
  |> filter(fn: (r) => uint(v: r["_time"]) >= 1724061418579000000 and uint(v: r["_time"]) < 1724061418580000000)
  |> sort(columns: ["_time"], desc: false)
'''

# 데이터 출력
tables = query_api.query(query)

for table in tables:
    for record in table.records: 
        # print({"time": record.get_time(), "text": record.get_value()})
        print({"value": record.get_value()})