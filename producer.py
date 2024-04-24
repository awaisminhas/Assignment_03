from kafka import KafkaProducer
import json
import time

# Specify the path to your preprocessed JSON file
json_file_path = '/home/malaika/Documents/output.json'

# Function to read preprocessed data from JSON file and stream it
def stream_preprocessed_data(json_file_path):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    
    with open(json_file_path, 'r') as file:
        data = json.load(file)
        for item in data:
            # Serialize data to JSON format and send it to Kafka topic
            producer.send('preprocessed_data_topic', json.dumps(item).encode('utf-8'))
            time.sleep(0.1)  # Adjust sleep time as needed for desired streaming speed

    producer.close()

# Start streaming preprocessed data
stream_preprocessed_data(json_file_path)

