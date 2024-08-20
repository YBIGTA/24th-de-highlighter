# Dependency

`pip3 install influxdb-client`

# Initialize 
```
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from dotenv import load_dotenv
from influxdb_client.client.write_api import SYNCHRONOUS

load_dotenv()

token = os.getenv("INFLUXDB_TOKEN")
org = os.getenv("INFLUXDB_ORG")
url = os.getenv("INFLUXDB_URL")

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
```

# Write Data
```
bucket=""

write_api = client.write_api(write_options=SYNCHRONOUS)
   
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
```
query_api = client.query_api()

query = """from(bucket: "HighLighter")
 |> range(start: -10m)
 |> filter(fn: (r) => r._measurement == "measurement1")"""
tables = query_api.query(query, org="HighLighter")

for table in tables:
  for record in table.records:
    print(record)
```