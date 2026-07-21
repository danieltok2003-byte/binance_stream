from confluent_kafka import Producer
import random
import time
import json
import asyncio
import logging
from dotenv import load_dotenv

load_dotenv()

producer_config = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(producer_config)

logging.basicConfig(level=logging.INFO)

SYMBOL = "BNBUSDT"
BASE_PRICE = 550.00  # rough starting point, doesn't need to be exact


def handle_data(data):
    logging.info(data)
    producer.produce(
        topic='orders',
        value=f"{data}",
    )


def generate_mock_agg_trade(symbol: str, price: float, trade_id: int) -> dict:
    """Builds a payload shaped like Binance's real aggTrade stream event."""
    now_ms = int(time.time() * 1000)
    qty = round(random.uniform(0.01, 5.0), 4)
    return {
        "e": "aggTrade",       # event type
        "E": now_ms,           # event time
        "s": symbol,           # symbol
        "a": trade_id,         # aggregate trade id
        "p": f"{price:.2f}",   # price
        "q": f"{qty}",         # quantity
        "f": trade_id,         # first trade id
        "l": trade_id,         # last trade id
        "T": now_ms,           # trade time
        "m": random.choice([True, False]),  # is buyer maker
    }


async def agg_trade(num_messages: int = 100, interval_seconds: float = 0.5):
    """Simulates a short burst of aggTrade messages, same shape as the real stream."""
    price = BASE_PRICE
    try:
        for i in range(num_messages):
            # small random walk so price looks plausible
            price += random.uniform(-5, 5)
            data = generate_mock_agg_trade(SYMBOL, price, trade_id=1000 + i)
            handle_data(json.dumps(data))
            await asyncio.sleep(interval_seconds)
    except Exception as e:
        logging.error(f"agg_trade() error: {e}")
    finally:
        producer.flush()


if __name__ == "__main__":
    asyncio.run(agg_trade())