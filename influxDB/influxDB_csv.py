import influxdb_client
import os
import pandas as pd
from datetime import datetime, timedelta
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# Load your data
file_path = 'data/normalized_parischim.csv'
df = pd.read_csv(file_path, header=None, names=['time', 'text'])

# Convert the 'time' column to timedelta
df['time'] = pd.to_timedelta(df['time'])

# Get the current UTC date (ignoring the time part)
current_utc_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

# InfluxDB connection details
token = os.environ.get("INFLUXDB_TOKEN")
org = "HighLighter"
url = "http://13.125.176.29:8086"
bucket = "HighLighter"

# Create InfluxDB client
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

# Create write API
write_api = client.write_api(write_options=SYNCHRONOUS)

# Write data to InfluxDB
for _, row in df.iterrows():
    # Add the timedelta to the current UTC date to get the correct timestamp
    timestamp = current_utc_date + row['time']

    # Create a point object for each row
    point = Point("parischim_data")\
        .field("text", row["text"])\
        .time(timestamp, WritePrecision.S)
      
    print(point)

    # Write the point to InfluxDB
    write_api.write(bucket=bucket, org=org, record=point)

# Close the client
client.close()