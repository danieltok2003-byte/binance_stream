import logging
from confluent_kafka import Producer
from dotenv import load_dotenv
import os
import asyncio
import requests
import json
import datetime
import time

load_dotenv()
key = os.environ.get('COINGECKO_DEMO_API_KEY')

producer_config = {
    # avoid having to rebuild image if just testing locally
    'bootstrap.servers': 'kafka:9092'
\
}

producer = Producer(producer_config)

logging.basicConfig(level=logging.INFO)


def handle_data(data, topic):
    logging.info(data)
    producer.produce(
        topic=topic,
        value=f"{data}",
    )


tracked_ids = "bitcoin,ethereum,tether,binancecoin,solana"

async def main():
    logging.info(f'Producer started. config: {producer_config} ')
    while True:
        r = requests.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": tracked_ids, "vs_currencies": "usd", 'include_last_updated_at': 'true'},
            headers={"x-cg-demo-api-key": key}
            # example return payload: {'bitcoin': {'usd': 63432}}
        )


        data = r.json()
        print(data)

        # reshaped = {
        #     's': list(data.keys())[0],
        #     'p': data[list(data.keys())[0]]['usd'],
        #     'q' : 1,
        #     'E' : datetime.datetime.now().timestamp()
        # }
        handle_data(json.dumps(data), 'trades.agg_trades')
        time.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())