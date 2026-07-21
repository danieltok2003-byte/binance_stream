from confluent_kafka import Consumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from datetime import datetime

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'order-tracker',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['orders'])

# --- InfluxDB setup ---
INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "my-super-secret-token"
INFLUX_ORG = "myorg"
INFLUX_BUCKET = "trades"

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

print("Consumer is running and subscribed to orders topic")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        value = msg.value().decode('utf-8')
        order = json.loads(value)

        price = float(order['p'])
        qty = float(order['q'])
        symbol = order['s']
        ts = datetime.fromtimestamp(order['E'] / 1000)  # E is ms; drop /1000 if you kept it in seconds

        print(f"Received order: {qty} x {symbol} @ ${price} {ts}")

        point = (
            Point("trades")
            .tag("symbol", symbol)
            .field("price", price)
            .field("quantity", qty)
            .time(ts)
        )
        try:
            write_api.write(bucket=INFLUX_BUCKET, record=point)
            print(f"Wrote point: {symbol} @ {price}")
        except Exception as e:
            print(f"INFLUX WRITE FAILED: {e}")

except KeyboardInterrupt:
    print('\n Stopping consumer')

finally:
    consumer.close()
    influx_client.close()