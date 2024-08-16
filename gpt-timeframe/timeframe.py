import openai
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import timedelta

# .env 파일으로부터 API 키를 가져온다
load_dotenv()
api_key = os.getenv('GPT_TOKEN')

# --------------------------------------------------------#
# 데이터를 읽고 필요한 시간 범위를 설정한다
# 정규화된 시간 열을 timedelta 형식으로 변환한다
file_path = 'data/normalized_parischim.csv'
df = pd.read_csv(file_path, header=None, names=['time', 'text'])

# timedelta 형식으로 변환하기 위해 시간 열을 파싱한다
df['time'] = pd.to_timedelta(df['time'])

# 필요한 시간 범위를 설정한다
target_time = pd.to_timedelta('00:05:00')  # 5분 기준으로 설정
start_time = target_time - timedelta(minutes=1)  # 1분 전
end_time = target_time + timedelta(minutes=1)    # 1분 후

# 필요한 시간 범위에 해당하는 행만 필터링한다
filtered_df = df[(df['time'] >= start_time) & (df['time'] <= end_time)]

# 해당 행을 출력한다
print(filtered_df[['time', 'text']])

# --------------------------------------------------------#
# # OpenAI API를 사용하기 위해 API 키를 설정하고 클라이언트를 생성한다
# openai.api_key = api_key
# client = openai()

# # API 호출을 통해 대화를 생성한다
# response = client.chat.completions.create(
#   model="gpt-4o-mini",
#   messages=[
#     {"role": "system", "content": "Who is Heungindaeyo?"}
#   ]
# )

# # 답변에 해당하는 내용을 출력한다
# print(response.choices[0].message.content)