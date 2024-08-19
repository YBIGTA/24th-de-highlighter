import boto3
import os
from botocore.exceptions import ClientError
import shlex
import subprocess
import logging
from dotenv import load_dotenv
import os

# 생성된 Mp4와 제목이 존재할 때 AWS S3에 업로드

# .env 파일 로드
load_dotenv()

# 환경 변수 불러오기
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION') # TODO 사용 안 할 경우 삭제하기

def upload_file(file_name, bucket, object_name=None):
    # AWS S3 엑세스 키를 가져온다.

    # boto3 라이브러리로 s3에 mp4 파일 업로드 
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    print("업로드 성공!")
    return True

# 유튜브 업로드를 위해 from .ts to .mp4 변환
# TODO 시간되면 함수화 하기   
mp4_path="/home/ec2-user/test/test_video_jm.mp4" # TODO 경로 재지정 필요
ts_path= "/home/ec2-user/test_video_jm.ts" # TODO 경로 재지정 필요
ffmpeg_cmd = f"ffmpeg -i {ts_path} -c copy {mp4_path}" # subprocess 라이브러리활용 CLI로 실행시키기 위한 command  
command1 = shlex.split(ffmpeg_cmd)
p1 = subprocess.run(command1, stdout=subprocess.PIPE, stderr=subprocess.PIPE) #sub terminal에서 변환 실행
print("second terminal running")

def ts_to_mp4(ts_path,mp4_path):

    ffmpeg_cmd = f"ffmpeg -i {ts_path} -c copy {mp4_path}"
    command1 = shlex.split(ffmpeg_cmd)
    try:
        response=subprocess.run(command1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except OSError as e:
        logging.error(e)
        return False
    print("변환 성공")
    return True

bucket_name="MY_BUCKET" # TODO 지현이 s3 bucket으로 교체 필요 + .env에서 불러오기
object_name="MY_OBJECT_NAME" # TODO 업로드용 mp4파일 이름 지정 필요 
upload_file(mp4_path, bucket_name, object_name)