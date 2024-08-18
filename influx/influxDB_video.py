import influxdb_client
import os
from dotenv import load_dotenv
import pandas as pd
from influxdb_client.client.write_api import SYNCHRONOUS  # Import SYNCHRONOUS
from datetime import timedelta
import csv
import sys
import base64

# Increase the CSV field size limit
csv.field_size_limit(sys.maxsize)


# .env 파일으로부터 API 키를 가져온다
load_dotenv()
token = os.getenv('INFLUXDB_TOKEN')

# InfluxDB connection details
org = "HighLighter"
url = "http://13.125.176.29:8086"
bucket = "Test"

# InfluxDB client 인스턴스를 생성한다
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

# 쿼리 API를 사용하기 위해 인스턴스를 생성한다
query_api = client.query_api()

# 쿼리 시작 시간을 설정한다
start_time = "-12h"  # One hour ago

# "kafka_data"으로부터 "value" 필드만 가져오는 Flux 쿼리를 작성한다
query = f'''
from(bucket: "{bucket}")
  |> range(start: {start_time})
  |> filter(fn: (r) => r["_measurement"] == "kafka_data")
  |> filter(fn: (r) => r["_field"] == "value")
  |> keep(columns: ["_time", "_value"])
'''

# 쿼리를 실행하고 결과를 가져온다
tables = query_api.query(query)

# pandas DataFrame로 변환한다
results = []
for table in tables:
    for record in table.records:
        results.append({"time": record.get_time(), "video": record.get_value()})

df = pd.DataFrame(results)


def decode_byte_to_ts(df):
  # --------------------------------------------------------#
  video_file_path = "2min.ts"
  # Process the data for 1 minutes from the first entry
  if not df.empty:
      first_entry_time = df['time'].iloc[0]
      end_entry_time = first_entry_time + timedelta(minutes=2)
      print(f"First entry time: {first_entry_time}, End time: {end_entry_time}")
      
      # Filter the DataFrame for the 1-minute window
      df_filtered = df[(df['time'] >= first_entry_time) & (df['time'] <= end_entry_time)]
      print(df_filtered)
      
      print(f"Processing data from {first_entry_time} to {end_entry_time}")
      
      # Write the filtered data to a file
      with open(video_file_path, "wb") as video_file:
          for _, row in df_filtered.iterrows():
              decoded_chunk = base64.b64decode(row['video'].encode())
              video_file.write(decoded_chunk)
      
      print("Video file created at vid.ts")
  else:
      print("No data found in the specified time range.")
  # --------------------------------------------------------#


# Call the function
decode_byte_to_ts(df)

# Close the client
client.close()