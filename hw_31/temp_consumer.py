import json
import psycopg2
from confluent_kafka import Consumer, KafkaException

conf = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'temperature_consumer_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['topic_temperature'])

db_conn = psycopg2.connect(
    host='localhost',
    database='testdb',
    user='admin',
    password='secret',
    port='5432'
)

cursor = db_conn.cursor()

def insert_into_postgres(data):
    query = """
                INSERT INTO temperature_readings (timestamp, value)
                VALUES (%s, %s)
            """
    cursor.execute(query, (data['timestamp'], data['value']))
    db_conn.commit()

try:
    print("[v] Consumer start working..")
    while True:
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue

        try:
            data = json.loads(msg.value().decode('utf-8'))
            print(f'[v] Message recieved: {data}')
            insert_into_postgres(data)
        except:
            print('[x] Error')

except KeyboardInterrupt:
    print("[x] Consumer stopped")

finally:
    consumer.close()
