from kafka import KafkaConsumer
import json
from pyfpgrowth import find_frequent_patterns

def process_preprocessed_data():
    try:
        consumer = KafkaConsumer('preprocessed_data', bootstrap_servers=['localhost:9092'],
                                 auto_offset_reset='earliest', group_id=None)
        transactions = []
        for message in consumer:
            data = json.loads(message.value.decode('utf-8'))
            transactions.append(data['categories'])
        
        # Use FP-Growth algorithm to find frequent itemsets
        patterns = find_frequent_patterns(transactions, 2)
        print("Frequent Itemsets (FP-Growth Algorithm):")
        for itemset, support in patterns.items():
            print(f"{itemset}: {support}")
    except Exception as e:
        print("An error occurred:", e)

if _name_ == "main":
    process_preprocessed_data()
