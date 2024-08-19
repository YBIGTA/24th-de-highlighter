import influxdb_client
import os
from dotenv import load_dotenv
import pandas as pd
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS  # Import SYNCHRONOUS
from openai import OpenAI
from datetime import timedelta
import json
import base64
import boto3
from botocore.exceptions import ClientError
import logging
import shlex
import subprocess
import csv
import sys

def get_timeframe(df, date, target_time, bounds):
    """
    주어진 시간 주변 맥락에 맞는 대화를 분석해서 시작 시간, 종료 시간, 주요 주제, 설명을 반환한다.
    """
    # .env 파일으로부터 API 키를 가져온다
    load_dotenv()
    api_key = os.getenv('GPT_TOKEN')
    
    # --------------------------------------------------------#
    # 필요한 시간 범위를 설정한다
    target_time = pd.to_timedelta(target_time)  # 10분 기준으로 설정
    start_time = target_time - timedelta(minutes=bounds)  # 5분 전
    end_time = target_time + timedelta(minutes=bounds)    # 5분 후
    print(start_time, end_time)

    # 필요한 시간 범위에 해당하는 행만 필터링한다
    filtered_df = df[(df['time'] >= start_time) & (df['time'] <= end_time)]

    # --------------------------------------------------------#
    # 타임스탬프가 있는 대화를 분석하기 위한 문자열을 생성한다
    dialogue_with_timestamps = "\n".join(
        f"{row['time']} - {row['text']}" for _, row in filtered_df.iterrows()
    )

    # 시간을 문자열로 변환한다
    target_time_str = str(target_time)
    # 모델에 제공할 프롬프트를 생성한다
    korean_prompt = (
        "다음과 같은 타임스탬프가 있는 대화를 제공되었을 때, 지정된 대상 시간 주변의 내용을 분석하십시오. "
        "대상 시간에서 다뤄진 주요 주제의 시작 및 종료 시간을 결정하고, 주요 주제를 알려주세요.\n\n"
        f"대상 시간: {target_time_str}\n\n"
        "대화:\n"
        f"{dialogue_with_timestamps}\n\n"
        f"다음 정보를 {date}T[HH:MM:SS]Z 형식의 시간과 함께 JSON 형식으로 제공해주세요:\n"
        "- start_time: 관련 시작 시간\n"
        "- end_time: 관련 종료 시간\n"
        "- main_topic: 주요 주제에 대한 간단한 설명. 이때, 주제 제목에 띄어쓰기 없이 해주세요.\n"
        "- explanation: 이러한 시간과 주제를 선택한 이유에 대한 간단한 설명\n\n"
        "JSON 형식으로 직접 응답해주세요. 추가적인 마크다운 형식이나 코드 블록 표시(```)를 사용하지 마세요."
    )

    # --------------------------------------------------------#
    # OpenAI API를 사용하기 위해 API 키를 설정하고 클라이언트를 생성한다
    client = OpenAI(api_key=api_key)

    # API 호출을 통해 대화를 생성한다
    response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": "당신은 타임스탬프가 있는 대화를 분석하는 어시스턴트입니다."},
        {"role": "user", "content": korean_prompt}
    ]
    )

    # 답변에 해당하는 내용을 출력한다
    return response.choices[0].message.content




# .env 파일으로부터 API 키를 가져온다
load_dotenv()
token = os.getenv('INFLUXDB_TOKEN')

# InfluxDB connection details
org = "HighLighter"
url = "http://13.125.176.29:8086"
bucket = "HighLighter"

# InfluxDB client 인스턴스를 생성하고, 쿼리 API를 사용하기 위해 인스턴스를 생성한다
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()


# "video_text_1"으로부터 "text" 필드만 가져오는 Flux 쿼리를 작성한다
start_time = "-3h"  # One day ago
query = f'''
from(bucket: "{bucket}")
  |> range(start: {start_time})
  |> filter(fn: (r) => r["_measurement"] == "video_text_1")
  |> filter(fn: (r) => r["_field"] == "text")
  |> keep(columns: ["_time", "_value"])
'''
tables = query_api.query(query)

# pandas DataFrame로 변환한다
results = []
for table in tables:
    for record in table.records:
        results.append({"time": record.get_time(), "text": record.get_value()})

df = pd.DataFrame(results)

# 'time' 필드를 timedelta 형식으로 변환한다
df['time'] = pd.to_datetime(df['time'], utc=True)
df['time'] = pd.to_timedelta(df['time'].dt.strftime('%H:%M:%S'))
print(df)



# 첫번째 타임스탬프 데이터를 가져오며, 00:00:00으로 취급한다
# "video_text_1"으로부터 첫번째 타임스탬프를 가져오는 Flux 쿼리를 작성한다
start_time = "-3h"  
first_query = f'''
from(bucket: "{bucket}")
  |> range(start: {start_time})
  |> filter(fn: (r) => r["_measurement"] == "video_text_1")
  |> filter(fn: (r) => r["_field"] == "text")
  |> first()
  |> keep(columns: ["_time"])
'''

first_time = query_api.query(first_query)
if first_time and len(first_time) > 0 and len(first_time[0].records) > 0:
    time_value = first_time[0].records[0].values["_time"]
    print(time_value)

    time_str = time_value.strftime('%H:%M:%S')
    
    first_time_delta = timedelta(hours=int(time_str[:2]), 
                           minutes=int(time_str[3:5]), 
                           seconds=int(time_str[6:]))
    
    print(first_time_delta)
else:
    print("No data found in the specified time range.")

# timeframe 함수를 사용하여 주어진 시간 주변의 맥락에 맞는 시간 범위를 가져온다
highlight_time = first_time_delta + timedelta(minutes=15)
date = time_value.date()
gpt_response = get_timeframe(df, date, target_time=highlight_time, bounds=5)
print(gpt_response)

# Close the client
client.close()


# ----------------------------------------------------------#
# ----------------------------------------------------------#
# ----------------------------------------------------------#
# 범위를 받았으니, 이제 이를 사용하여 비디오를 추출하고, 변환하고, 업로드한다.#
# ----------------------------------------------------------#
# ----------------------------------------------------------#
# ----------------------------------------------------------#

# CSV의 필드 크기 제한을 해제한다
csv.field_size_limit(sys.maxsize)


# InfluxDB client 인스턴스를 생성하고, 쿼리 API를 사용하기 위해 인스턴스를 생성한다
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()

# "kafka_data"으로부터 "value" 필드만 가져오는 Flux 쿼리를 작성한다
# Assuming 'response' is the string containing the GPT's JSON response
gpt_data = json.loads(gpt_response)

query = f'''
from(bucket: "{bucket}")
  |> range(start: {gpt_data['start_time']}, stop: {gpt_data['end_time']})
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
  video_file_path = f"{gpt_data['main_topic']}.ts"
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


mp4_path=f"{gpt_data['main_topic']}.mp4"
ts_path= f"{gpt_data['main_topic']}.ts"

bucket_name="ybigta-highlight"
object_name=f"{gpt_data['main_topic']}.mp4"



# 실행코드: 바이트를 .ts로 디코딩하고 .ts를 .mp4로 변환하여 s3에 업로드
decode_byte_to_ts(df)
ts_to_mp4(ts_path,mp4_path)
upload_file(mp4_path, bucket_name, object_name)


# 클라이언트를 종료
client.close()