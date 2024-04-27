from kafka import KafkaConsumer
import json
import itertools
from pymongo import MongoClient
import pymongo
import uuid  # Import uuid module for generating unique identifiers

# MongoDB Setup
client = MongoClient('mongodb://localhost:27017')
db = client['kafka_data']
collection = db['association_rules']

# Ensure MongoDB collection exists
collection.create_index([('rule', pymongo.ASCENDING)], unique=True)

for i in range(10):
    key_name = f'confidence_{i}'  # Generate the key name
    unique_id = str(uuid.uuid4())  # Convert UUID to string
    chunk_data = {
        'rule': unique_id,  # Use the string representation of UUID as the rule
        key_name: "awais"  # Use the generated key name
    }
    print(chunk_data)

    collection.insert_one(chunk_data)

