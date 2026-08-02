from confluent_kafka import Producer
import random
import time
import json
import asyncio
import logging
from dotenv import load_dotenv

load_dotenv()

producer_config = {
    'bootstrap.servers': 'kafka:9092'
}

producer = Producer(producer_config)

logging.basicConfig(level=logging.INFO)

SYMBOL = "BNBUSDT"


SYMBOLS = ["BNBUSDT", "BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"]

class MockSymbolBehavior:
    def __init__(self, name, current_price, deviation):
        self.name = name
        self.current_price = current_price
        self.deviation = deviation

SYMBOLS = [
    MockSymbolBehavior('BNBUSDT', 500, 90),
    MockSymbolBehavior('BTCUSDT', 1000, 50),
    MockSymbolBehavior('ETHUSDT', 800, 100),
    MockSymbolBehavior('SOLUSDT', 200, 40),
    MockSymbolBehavior('DOGEUSDT', 300, 50)
]



BASE_PRICE = 550.00  # rough starting point, doesn't need to be exact


def handle_data(data, topic):
    logging.info(data)
    producer.produce(
        topic=topic,
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



async def agg_trade(num_messages: int = 100, interval_seconds: float = 1):
    """Simulates a short burst of aggTrade messages, same shape as the real stream."""
    i = 0
    try:
        # for i in range(num_messages):
        while True:
            # small random walk so price looks plausible
            random_symbol = random.choice(SYMBOLS)
            
            random_symbol.current_price += random.uniform(-random_symbol.deviation, random_symbol.deviation)
            data = generate_mock_agg_trade(random_symbol.name, random_symbol.current_price, trade_id=1000 + i)
            i += 1
            handle_data(json.dumps(data), 'trades.agg_trades')
            await asyncio.sleep(interval_seconds)
    except Exception as e:
        logging.error(f"agg_trade() error: {e}")
    finally:
        producer.flush()

    
async def avg_price(num_messages: int = 100, interval_seconds: float = 1):
    """Simulates a short burst of avg price messages, same shape as the real stream."""
    i = 0
    try:
        # for i in range(num_messages):
        while True:
            # small random walk so price looks plausible
            random_symbol = random.choice(SYMBOLS)
            
            random_symbol.current_price += random.uniform(-random_symbol.deviation, random_symbol.deviation)
            data = generate_mock_agg_trade(random_symbol.name, random_symbol.current_price, trade_id=1000 + i)
            data = generate_mock_avg_price(random_symbol, 500)
            i += 1
            handle_data(json.dumps(data), 'trades.avg_price')
            await asyncio.sleep(interval_seconds)
    except Exception as e:
        logging.error(f"agg_trade() error: {e}")
    finally:
        producer.flush()


def generate_mock_avg_price(symbol: str, price: float) -> dict:
    """Builds a payload shaped like Binance's real average price stream event."""
    now_ms = int(time.time() * 1000)
    avg = price + random.uniform(-price * 0.001, price * 0.001)
    return {
        "e": "avgPrice",        # event type
        "s": symbol,            # symbol
        "i": "5m",               # average price interval
        "w": f"{avg:.2f}",       # average price
        "T": now_ms,             # last trade time
    }


if __name__ == "__main__":
    async def main():
        await asyncio.gather(
            agg_trade(),
            
        )