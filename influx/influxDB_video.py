import influxdb_client
import os
from dotenv import load_dotenv
import pandas as pd
from influxdb_client.client.write_api import SYNCHRONOUS  # Import SYNCHRONOUS
from datetime import timedelta
import csv
import sys
import base64
import boto3
from botocore.exceptions import ClientError
import logging
import shlex
import subprocess

# CSV의 필드 크기 제한을 해제한다
csv.field_size_limit(sys.maxsize)


# .env 파일으로부터 API 키를 가져온다
load_dotenv()
token = os.getenv('INFLUXDB_TOKEN')

# InfluxDB 설정
org = "HighLighter"
url = "http://13.125.176.29:8086"
bucket = "HighLighter"

# InfluxDB client 인스턴스를 생성하고, 쿼리 API를 사용하기 위해 인스턴스를 생성한다
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()
start_time = "-2h"

# "kafka_data"으로부터 "value" 필드만 가져오는 Flux 쿼리를 작성한다
query = f'''
from(bucket: "{bucket}")
  |> range(start: {start_time})
  |> filter(fn: (r) => r["_measurement"] == "video_binary_2")
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
  """
  바이트를 .ts로 디코딩하여 파일로 저장한다.
  """
  video_file_path = "obama.ts"
  if not df.empty:
      first_entry_time = df['time'].iloc[0]
      end_entry_time = first_entry_time + timedelta(minutes=30)
      print(f"First entry time: {first_entry_time}, End time: {end_entry_time}")
      
      # 설정한 시간 범위 내의 데이터만 필터링한다
      df_filtered = df[(df['time'] >= first_entry_time) & (df['time'] <= end_entry_time)]
      print(df_filtered)
      
      print(f"Processing data from {first_entry_time} to {end_entry_time}")
      
      # Write the filtered data to a file
      with open(video_file_path, "wb") as video_file:
          for _, row in df_filtered.iterrows():
              decoded_chunk = base64.b64decode(row['video'].encode())
              video_file.write(decoded_chunk)
      
      print(f"Video file created at {video_file_path}")
  else:
      print("No data found in the specified time range.")



def ts_to_mp4(ts_path,mp4_path):
  """
  유튜브 업로드를 위해 from .ts to .mp4 변환
  """
  ffmpeg_cmd = f"ffmpeg -i {ts_path} -c copy {mp4_path}"
  command1 = shlex.split(ffmpeg_cmd)
  try:
      response=subprocess.run(command1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  except OSError as e:
      logging.error(e)
      return False
  print("변환 성공")
  return True


def upload_file(file_name, bucket, object_name=None):
  """
  AWS S3 엑세스 키를 가져온다.
  """

  # boto3 라이브러리로 s3에 mp4 파일 업로드 
  s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
  try:
      response = s3_client.upload_file(file_name, bucket, object_name)
  except ClientError as e:
      logging.error(e)
      return False
  print("업로드 성공!")
  return True



# 환경 변수 불러오기
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = "ap-northeast-2"


mp4_path="obama.mp4"
ts_path= "obama.ts"

bucket_name="ybigta-highlight"
object_name="obama.mp4"



# 실행코드: 바이트를 .ts로 디코딩하고 .ts를 .mp4로 변환하여 s3에 업로드
decode_byte_to_ts(df)
ts_to_mp4(ts_path,mp4_path)
# upload_file(mp4_path, bucket_name, object_name)


# 클라이언트를 종료
client.close()