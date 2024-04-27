import json
import threading
import time
from kafka import KafkaProducer

# Load data from the JSON file
with open('/home/muhammad/preprocessed_data.json', 'r') as file:
    data = json.load(file)

bootstrap_servers = ['localhost:9092']
topic = 'topic01'

# Define the producer function
def produce_data(data, chunk_size, producer):
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        combined_lists = []
        for entry in chunk:
            also_buy_list = entry.get('also_buy', [])
            combined_lists.append(also_buy_list)
        producer.send(topic, value=combined_lists)
        print("List of lists sent to Kafka topic:", combined_lists)
        time.sleep(60)  # Simulating delay

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Create and start the producer thread
producer_thread = threading.Thread(target=produce_data, args=(data, 12, producer))
producer_thread.start()

# Wait for the producer thread to finish
producer_thread.join()

