from confluent_kafka import Consumer
import json

consumer_config = {
    'bootstrap.servers' : 'localhost:9092',
    'group.id' : 'order-tracker', # unique str identifying consumer group consumer belongs to (same program running in group)
    'auto.offset.reset': 'earliest' # what to do if no initial offset/current offset got deleted (earliest- reset to earliest offset, latest->vice versa, by_duration:<duration> - reset offset to configured duration, none (throw execption))
}

consumer = Consumer(consumer_config)

consumer.subscribe(['orders'])

print("Consumer is runnning and subscribed to orders topic")

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
        print(f"Received order: {order['quantity']} x {order['item']} from {order['user']}")
except KeyboardInterrupt:
    print('\n Stopping consumer')

finally:
    consumer.close()