import influxdb_client
import os
import pandas as pd
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS  # Import SYNCHRONOUS

# InfluxDB connection details
token = os.environ.get("INFLUXDB_TOKEN")
org = "HighLighter"
url = "http://13.125.176.29:8086"
bucket = "HighLighter"


# Create InfluxDB client
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)

# Create Query API
query_api = client.query_api()

# Define the time range for the query
start_time = "-1h"  # One hour ago
end_time = "now()"  # Current time



# Flux query to retrieve all data from the "parischim_data" measurement
query = f'''
from(bucket: "{bucket}")
  |> range(start: {start_time}, stop: {end_time})
  |> filter(fn: (r) => r["_measurement"] == "parischim_data")
  |> filter(fn: (r) => r["_field"] == "text")
  |> keep(columns: ["_time", "_value"])
'''

# Execute the query
tables = query_api.query(query)

# Convert the results to a pandas DataFrame
results = []
for table in tables:
    for record in table.records:
        results.append({"time": record.get_time(), "text": record.get_value()})

df = pd.DataFrame(results)

# Display the resulting DataFrame
print(df)

# Close the client
client.close()