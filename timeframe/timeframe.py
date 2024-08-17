from openai import OpenAI
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import timedelta

# .env 파일으로부터 API 키를 가져온다
load_dotenv()
api_key = os.getenv('GPT_TOKEN')

def timeframe(df, target_time, bounds):
    # --------------------------------------------------------#
    # 필요한 시간 범위를 설정한다
    target_time = pd.to_timedelta(target_time)  # 10분 기준으로 설정
    start_time = target_time - timedelta(minutes=bounds)  # 5분 전
    end_time = target_time + timedelta(minutes=bounds)    # 5분 후

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