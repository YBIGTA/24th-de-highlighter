## Run
```bash
python main.py <Livestream URL> <Buffer size> <Time duration>
ffmpeg -i vid.ts -c copy <Video filename>
```
- 버퍼 크기: 131072 (128KB)
- 시간 제한: 무제한

## 버퍼 크기

버퍼 크기 별 (총 데이터 크기) / (버퍼 크기 * 읽은 횟수) 값

|`BUF_SIZE`|Utilization|
|-|-|
|4 KB|0.9980|
|64 KB|0.9677|
|128 KB|0.9523|
|512 KB|0.8093|

버퍼 크기를 키우면 카프카에 전송되는 메시지의 개수를 줄일 수 있음. 위에서 수치가 떨어지는 건 아마 스트림 받아오는 속도가 읽는 속도를 못 따라가서 그런듯 함.

Note: 카프카 메시지 크기 한도는 1 MB
