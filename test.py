from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

client = InfluxDBClient(
    url="http://localhost:8086",
    token="my-super-secret-token",
    org="myorg"
)
write_api = client.write_api(write_options=SYNCHRONOUS)

point = Point("trades").tag("symbol", "TEST").field("price", 123.45).time(datetime.utcnow())

try:
    write_api.write(bucket="trades", record=point)
    print("Write succeeded")
except Exception as e:
    print(f"Write failed: {e}")

client.close()