from confluent_kafka import Consumer, Producer
import json

conf_consumer = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'stream_processor_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf_consumer)
consumer.subscribe(['raw_data'])

conf_producer = {
    'bootstrap.servers': 'localhost:9094'
}

producer = Producer(conf_producer)

try:
    print("[v] Stream processor start working..")
    while True:
        msg = consumer.poll(timeout=1.0)  
        
        if msg is None:
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Processing: {data}")
            
            if data['type'] == 'temperature':
                producer.produce('topic_temperature', json.dumps(data).encode('utf-8'))
                print(f"Sent to temperature topic: {data}")
            elif data['type'] == 'humidity':
                producer.produce('topic_humidity', json.dumps(data).encode('utf-8'))
                print(f"Sent to humidity topic: {data}")
            
            producer.flush()  
        except:
            print('[x] Error')
except KeyboardInterrupt:
    print("Stream processor stopped")
finally:
    consumer.close()