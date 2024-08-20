# InfluxDB vs RDB
| Database | Databse |
| --- | --- |
| Table | Measurement |
| Column | Key |
| Indexed Column | Tag Key (String Only) |
| Unindexed Column | Field Key |
| Row | Point |

# Dependency

`pip3 install influxdb-client`

# Initialization
```python
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from dotenv import load_dotenv
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url = os.getenv("INFLUXDB_URL")
bucket = os.getenv("INFLUXDB_BUCKET")

# 클라이언트 생성
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
```

# Write Data
- InfluxDB에서는 Write할 API와 Read할 API를 각각 생성해줘야된다.

예시는 아래와 같다:
```python
# Write/Insert API 생성
write_api = client.write_api(write_options=SYNCHRONOUS)
   
# Write할 시, point라는 것을 활용해 스키마를 짜줘서, Write해줘야 된다.
# 형식은 아래와 같다.
for value in range(5):
  point = (
    Point("measurement1")
    .tag("tagname1", "tagvalue1")
    .field("field1", value)
  )
  write_api.write(bucket=bucket, org="HighLighter", record=point)
  time.sleep(1) # separate points by 1 second
```

# Query Data

```python
# Read하는 API를 생성해준다
query_api = client.query_api()

# 10분 전부터 현재 시간까지의 데이터들을 가져오는 쿼리
query = """from(bucket: "HighLighter")
 |> range(start: -10m)
 |> filter(fn: (r) => r._measurement == "measurement1")"""
tables = query_api.query(query, org="HighLighter")

# 출력해보자
for table in tables:
  for record in table.records:
    print(record)
```
