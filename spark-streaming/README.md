### Spark Streaming

1. Kafka topic에 적재된 video binary를 spark에 저장
2. spark window를 사용하여 30초 분량의 binary를 적재하고 이를 배치 단위로 전처리
    1) 30초의 binary를 합쳐 video.ts로 저장 → ffmpeg로 video.ts를 video.mp4로 변환
    2) whisper를 사용하여 mp4의 음성을 text로 전사
    3) spark dataframe에 전사된 text를 새로운 column에 추가
3. spark query를 사용하여 전처리된 배치에 대해 foreachBatch()를 사용하여 influxdb에 적재