import os
import random
import time
import boto3
import google.auth
import google.auth.transport.requests
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
from tempfile import NamedTemporaryFile

# Secrets Manager 설정
OAUTH2_SECRET_NAME = "upload_video.py-oauth2.json"
CLIENT_SECRET_NAME = "client_secrets.json"
REGION_NAME = "ap-northeast-2"

# YouTube API 설정
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

MAX_RETRIES = 10
RETRIABLE_STATUS_CODES = [500, 502, 503, 504]

def get_secret(secret_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=REGION_NAME)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except boto3.exceptions.Boto3Error as e:
        raise e
    return get_secret_value_response['SecretString']

def save_secret_to_file(secret, file_path):
    with open(file_path, 'w') as secret_file:
        secret_file.write(secret)

def get_authenticated_service():
    oauth2_credentials_json = get_secret(OAUTH2_SECRET_NAME)
    client_secret_json = get_secret(CLIENT_SECRET_NAME)
    
    oauth2_file_path = "/tmp/upload_video-oauth2.json"
    client_secret_file_path = "/tmp/client_secrets.json"
    
    save_secret_to_file(oauth2_credentials_json, oauth2_file_path)
    save_secret_to_file(client_secret_json, client_secret_file_path)

    creds = Credentials.from_authorized_user_file(oauth2_file_path)
    if not creds.valid:
        creds.refresh(google.auth.transport.requests.Request())

    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, credentials=creds)

def initialize_upload(youtube, options):
    tags = None
    if 'keywords' in options:
        tags = options['keywords'].split(",")

    body = dict(
        snippet=dict(
            title=options.get('title', "Test Title"),
            description=options.get('description', "Test Description"),
            tags=tags,
            categoryId=options.get('category', "22")
        ),
        status=dict(
            privacyStatus=options.get('privacyStatus', "private")
        )
    )

    insert_request = youtube.videos().insert(
        part=",".join(list(body.keys())),
        body=body,
        media_body=MediaFileUpload(options['file'], chunksize=-1, resumable=True)
    )

    resumable_upload(insert_request)

def resumable_upload(insert_request):
    response = None
    error = None
    retry = 0
    while response is None:
        try:
            print("Uploading file...")
            status, response = insert_request.next_chunk()
            if response is not None:
                if 'id' in response:
                    print(f"Video id '{response['id']}' was successfully uploaded.")
                else:
                    raise Exception(f"The upload failed with an unexpected response: {response}")
        except HttpError as e:
            if e.resp.status in RETRIABLE_STATUS_CODES:
                error = f"A retriable HTTP error {e.resp.status} occurred:\n{e.content}"
            else:
                raise
        except Exception as e:
            error = f"A retriable error occurred: {e}"

        if error is not None:
            print(error)
            retry += 1
            if retry > MAX_RETRIES:
                raise Exception("No longer attempting to retry.")

            max_sleep = 2 ** retry
            sleep_seconds = random.random() * max_sleep
            print(f"Sleeping {sleep_seconds} seconds and then retrying...")
            time.sleep(sleep_seconds)


def lambda_handler(event, context):
    print(f"Event data: {event}")  # 이벤트 데이터 로깅
    
    # S3 버킷 이름과 객체 키 추출
    try:
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']
    except KeyError as e:
        raise ValueError(f"Failed to extract S3 bucket or key: {str(e)}")
    
     # S3 객체 키에서 파일 이름 추출
    file_name = os.path.basename(s3_key)
    title = os.path.splitext(file_name)[0]  # 확장자를 제거한 파일 이름을 title로 사용

    description = event.get('description', "Test Description")
    category = event.get('category', "22")
    keywords = event.get('keywords', "")
    privacy_status = event.get('privacyStatus', "private")

    if not s3_bucket or not s3_key:
        raise ValueError("S3 bucket and key are required in the event data.")

    s3 = boto3.client('s3')
    with NamedTemporaryFile(delete=False) as temp_file:
        s3.download_fileobj(s3_bucket, s3_key, temp_file)
        temp_file_path = temp_file.name

    youtube = get_authenticated_service()

    options = {
        "file": temp_file_path,
        "title": title,
        "description": description,
        "category": category,
        "keywords": keywords,
        "privacyStatus": privacy_status
    }

    try:
        initialize_upload(youtube, options)
    except HttpError as e:
        print(f"An HTTP error {e.resp.status} occurred:\n{e.content}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    os.remove(temp_file_path)  # 임시 파일 정리

    return {
        'statusCode': 200,
        'body': 'Video uploaded successfully!'
    }
