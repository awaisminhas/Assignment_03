from kafka import KafkaConsumer
import json

def consume_data():
    consumer = KafkaConsumer('preprocessed_data_topic', bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest', group_id=None)

    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        # Process data (e.g., perform analytics or further preprocessing)
        print("Consumer 2 received data:", data)

if __name__ == "__main__":
    consume_data()

