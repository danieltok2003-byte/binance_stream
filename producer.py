from confluent_kafka import Producer
import uuid
import json

producer_config = {
    'bootstrap.servers' : 'localhost:9092' # define 1 broker as starting point to discover full list of servers in cluster
}

producer = Producer(producer_config)

order = {
    'order_id': str(uuid.uuid4()),
    'user': 'will',
    'item': 'chicken burger',
    'quantity': 1
}

value = json.dumps(order).encode('utf-8')

def delivery_report(err, msg):
    if err:
        print(f'Delivery failed: {err}')
    else:
        print(f"Delivery succeeded: {msg.value().decode('utf-8')}")
        print(f"Delivered to topic {msg.topic()}, partition: {msg.partition()} at offset: {msg.offset()}")

producer.produce(
    topic='orders', 
    value=value,
    callback=delivery_report # call after deliver
)

producer.flush() # group and send in batch, not 1 by 1 (if fail, before terminate sends unsent - always call before end)





