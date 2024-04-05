## Youtube-live-chat
실시간 유튜브 영상 url이 주어질 때 해당 영상의 livechat을 /chat 에 json형식으로 저장한다.

### 실행
1. .env 파일을 생성하고 GCP KEY를 넣어준다.
2. docker-compose.yaml에 youtube live url을 넣어준다. 
3. docker-compose를 실행한다.
    ```
    docker-compose up --build
    ```