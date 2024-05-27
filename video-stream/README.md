## Overview
<img src="img/flow.svg">

- [x] Streamlink on EC2
- [x] EC2에서 SQS로 메시지 전송하기
- [x] Lambda에서 컨슈머 함수 구현
- [ ] Lambda 트리거 설정
- [x] S3에 영상 저장

## Run
```bash
python main.py <Livestream URL> <Buffer size> <Time duration>
ffmpeg -i vid.ts -c copy <Video filename>
```
- 버퍼 크기: 131072 (128KB)
- 시간 제한: 무제한

## Buffer Size

버퍼 크기 별 (총 데이터 크기) / (버퍼 크기 * 읽은 횟수) 값. 5분 단위로 측정.

|`BUF_SIZE`|Utilization|
|-|-|
|4 KB|0.9980|
|64 KB|0.9677|
|128 KB|0.9523|
|512 KB|0.8093|

크기를 키우면 SQS에 전송되는 메시지의 개수를 줄일 수 있음. 하지만 크기를 너무 키우면 스트림 받아오는 속도보다 읽는 속도가 빨라져서 빈 공간(?)이 많이 생기는 것 같음. 참고로 SQS의 메시지 크기 제한은 256KB.
