from confluent_kafka import Consumer
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import json
from datetime import datetime, timezone
import logging
import os
logging.basicConfig(level=logging.INFO)
consumer_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'order-tracker',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_config)
consumer.subscribe(['trades.agg_trades'])

INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = "my-super-secret-token"
INFLUX_ORG = "myorg"
INFLUX_BUCKET = "trades"

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

logging.info("Consumer is running and subscribed to trades.agg_trades topic")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        value = msg.value().decode('utf-8')
        symbols = json.loads(value)


        points = []
        logging.info('================')
        logging.info(symbols)
        for symbol, metadata in symbols.items():
            price = float(metadata['usd'])
            ts = datetime.fromtimestamp(metadata['last_updated_at'], tz=timezone.utc)
            point = (
                Point("trades")
                .tag("symbol", symbol)
                .field("price", price)
                .time(ts)
            )
            logging.info(f"{symbol} @ ${price} {ts}")
            points.append(point)
        try:
            write_api.write(bucket=INFLUX_BUCKET, record=points)
            logging.info(f"Wrote point: {symbol} @ {price}")
        except Exception as e:
            logging.info(f"INFLUX WRITE FAILED: {e}")

except KeyboardInterrupt:
    logging.info('\n Stopping consumer')

finally:
    consumer.close()
    influx_client.close()