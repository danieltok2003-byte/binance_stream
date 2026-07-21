from confluent_kafka import Producer
import uuid
import json
import asyncio
import logging
from dotenv import load_dotenv
from binance_common.configuration import ConfigurationWebSocketStreams
from binance_common.constants import SPOT_WS_STREAMS_PROD_URL
from binance_sdk_spot.spot import Spot

load_dotenv()

producer_config = {
    'bootstrap.servers' : 'kafka:9092' # define 1 broker as starting point to discover full list of servers in cluster
}

producer = Producer(producer_config)

logging.basicConfig(level=logging.INFO)

configuration_ws_streams = ConfigurationWebSocketStreams(
    stream_url=SPOT_WS_STREAMS_PROD_URL,
)

client = Spot(config_ws_streams=configuration_ws_streams)

def handle_data(data):
    logging.info(data)
    producer.produce(
        topic='orders', 
        value=f"{data}",
    )
async def agg_trade():
    connection = None
    try:
        connection = await client.websocket_streams.create_connection()

        stream = await connection.agg_trade(
            symbol="bnbusdt",
        )
        stream.on("message", handle_data)
        

        await asyncio.sleep(5)
        await stream.unsubscribe()
    except Exception as e:
        logging.error(f"agg_trade() error: {e}", exc_info=True)
    finally:
        if connection:
            
            await connection.close_connection(close_session=True)
            producer.flush() # group and send in batch, not 1 by 1 (if fail, before terminate sends unsent - always call before end

if __name__ == "__main__":
    asyncio.run(agg_trade())





