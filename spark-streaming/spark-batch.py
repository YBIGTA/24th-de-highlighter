from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, collect_list, udf
from pyspark.sql.types import StringType
import subprocess
import whisper
import uuid
import shlex
from pyspark.sql.streaming import StreamingQueryListener

import os
import pandas as pd
import time
import json
from dotenv import load_dotenv
import influxdb_client
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

import os
import shutil

# create checkpoint folder
folder_path = 'checkpoint'
if not os.path.exists(folder_path):
    os.makedirs(folder_path)
else:
    shutil.rmtree(folder_path)
    os.makedirs(folder_path)

# load InfluxDB env setting
load_dotenv()

token = os.environ.get("INFLUXDB_TOKEN")
org = os.environ.get("INFLUXDB_ORG")
url = os.environ.get("INFLUXDB_URL")
bucket = os.environ.get("INFLUXDB_BUCKET")

# Load the Whisper model
stt_model = whisper.load_model("base")

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# create influx client
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Define custom query listener
class QueryListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"Query started: id = {event.id}, name = {event.name}")

    def onQueryProgress(self, event):
        print(f"Query progress: id = {event}")

    def onQueryTerminated(self, event):
        print(f"Query terminated: id = {event}")

spark.streams.addListener(QueryListener())


def concat_byte_arrays(byte_array_list):
    binary_video_frames = b''.join(byte_array_list)
    u_key = str(uuid.uuid4())

    #make ts
    with open(f"video/output_{u_key}.ts", "wb") as video_file:
        video_file.write(binary_video_frames)

    try:
        ffmpeg_cmd = f"ffmpeg -i video/output_{u_key}.ts -c copy video/output_{u_key}.mp4"
        subprocess.run(shlex.split(ffmpeg_cmd), check=True)
    except Exception as e:
        print(f"Error: {e}")

    result = whisper.transcribe(stt_model, f"video/output_{u_key}.mp4")
    return result["text"]

concat_udf = udf(concat_byte_arrays, StringType())


# Read kafka topic and save in spark
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "52.79.81.77:9092") \
    .option("kafka.group.id", "spark") \
    .option("subscribe", "mytopic") \
    .load()

df.selectExpr("value as video_frames", "timestamp as timestamp")


# aggregate 30 seconds of video byte array by using truncated window (not overlaped)
# and translate binary array to mp4, do stt after that and save into concatenated_frames
window_df = df \
    .withWatermark("timestamp", "30 seconds") \
    .groupBy(
        window(col("timestamp"), "30 seconds")) \
    .agg(concat_udf(collect_list("value")).alias("concatenated_frames"))


def write_to_influxdb(batch_df, batch_id):
    
    for row in batch_df.collect():
        text_data = row["concatenated_frames"]
        timestamp = row["window"].start  

        print("batch_id", batch_id)
        print("text_data", text_data)
        
        point = (
            Point("video_text")
            .field("text", text_data)
            .time(timestamp, WritePrecision.S)
        )
        
        write_api.write(bucket=bucket, org=org, record=point)
        
        u_key = str(uuid.uuid4())

        with open(f"test/{u_key}.json", "w", encoding="utf-8") as txt_file:
            data = {
                    "timestamp": str(timestamp),
                    "text": text_data
                }
            json.dump(data, txt_file, ensure_ascii=False, indent=4)


# for check: save data 
query = window_df \
    .writeStream \
    .foreachBatch(write_to_influxdb) \
    .start()

query.awaitTermination()

client.close()