import openai
from dotenv import load_dotenv
import os
import pandas as pd

# .env 파일으로부터 API 키를 가져온다
load_dotenv()
api_key = os.getenv('GPT_TOKEN')

# OpenAI API를 사용하기 위해 API 키를 설정하고 클라이언트를 생성한다
openai.api_key = api_key
client = openai()

# API 호출을 통해 대화를 생성한다
response = client.chat.completions.create(
  model="gpt-4o-mini",
  messages=[
    {"role": "system", "content": "Who is Heungindaeyo?"}
  ]
)

# 답변에 해당하는 내용을 출력한다
print(response.choices[0].message.content)