import influxdb_client
import os
from dotenv import load_dotenv
import pandas as pd
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS  # Import SYNCHRONOUS
from openai import OpenAI
from datetime import timedelta

def get_timeframe(df, target_time, bounds):
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
        "다음 형식에 따라 분석을 제공해주세요:\n"
        "1. 관련 시작 시간: [hh:mm:ss]\n"
        "2. 관련 종료 시간: [hh:mm:ss]\n"
        "3. 주요 주제: [주요 주제에 대한 간단한 설명]\n"
        "4. 설명: [이러한 시간과 주제를 선택한 이유에 대한 간단한 설명]"
    )

    # --------------------------------------------------------#
    # OpenAI API를 사용하기 위해 API 키를 설정하고 클라이언트를 생성한다
    OpenAI.api_key = api_key
    client = OpenAI()

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

# InfluxDB client 인스턴스를 생성한다
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

# 쿼리 API를 사용하기 위해 인스턴스를 생성한다
query_api = client.query_api()

# 쿼리 시작 시간을 설정한다
start_time = "-12h"  # One hour ago

# "parischim_data"으로부터 "text" 필드만 가져오는 Flux 쿼리를 작성한다
query = f'''
from(bucket: "{bucket}")
  |> range(start: {start_time})
  |> filter(fn: (r) => r["_measurement"] == "parischim_data")
  |> filter(fn: (r) => r["_field"] == "text")
  |> keep(columns: ["_time", "_value"])
'''

# 쿼리를 실행하고 결과를 가져온다
tables = query_api.query(query)

# pandas DataFrame로 변환한다
results = []
for table in tables:
    for record in table.records:
        results.append({"time": record.get_time(), "text": record.get_value()})

df = pd.DataFrame(results)

# Convert the 'time' column to datetime if it's not already
df['time'] = pd.to_datetime(df['time'], utc=True)

# Convert to Timedelta representing time of day
df['time'] = pd.to_timedelta(df['time'].dt.strftime('%H:%M:%S'))

# timeframe/timeframe.py에 있는 timeframe 함수를 호출한다
print(get_timeframe(df, target_time='00:10:00', bounds=5))

# Close the client
client.close()