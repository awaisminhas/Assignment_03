from kafka import KafkaConsumer
import uuid
import json
import itertools
from pymongo import MongoClient
import pymongo

# MongoDB Setup
client = MongoClient('mongodb://localhost:27017')
db = client['kafka_data']
collection = db['association_rules']

# Ensure MongoDB collection exists
collection.create_index([('rule', pymongo.ASCENDING)], unique=True)

bootstrap_servers = ['localhost:9092']
topics = ['topic01']
local_path = '/home/muhammad/kafka/awais.txt'

consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, value_deserializer=lambda x: json.loads(x.decode('utf-8')))

consumer.subscribe(topics)

# Open the file in append mode ('a') to write to it
for message in consumer:
    data = message.value
    distinct_items = set()
    for sublist in data:
        distinct_items.update(sublist)
    distinct_items_list = list(distinct_items)

    combinations_list = []

    for r in range(1, min(len(distinct_items), 3) + 1):
        combinations = itertools.combinations(distinct_items, r)
        combinations_list.extend(list(combinations))

    item_sets_count = {}
    for item_set in combinations_list:
        item_set = tuple(item_set)
        for basket in data:
            if set(item_set).issubset(basket):
                if item_set not in item_sets_count:
                    item_sets_count[item_set] = 1
                else:
                    item_sets_count[item_set] += 1

    item_counts = {}
    for transaction in data:  # Increment count for each item in the transaction
        for item in transaction:
            item_counts[item] = item_counts.get(item, 0) + 1
    min_support = 2

    frequent_itemsets = []
    # Output frequent itemsets of size k
    for item, count in item_counts.items():
        if count >= min_support:
            frequent_itemsets.append(item)

    def generate_candidate_itemsets(itemsets, k):
        candidates = []
        # Generate combinations of size k
        # Convert tuples to 1D list if present
        if any(isinstance(item, (tuple, list)) for item in itemsets):
            itemsets = [item for sublist in itemsets for item in sublist]

        # Convert itemsets to set to remove duplicates
        itemsets = set(itemsets)

        # Generate combinations of size k
        for subset in itertools.combinations(itemsets, k):
            candidates.append(subset)
        return candidates

    frequent_itemsets_dict = {item: count for item, count in item_counts.items() if count >= min_support}

    sets_for_association_rules = {}

    k = 1
    discarded = set()

    while True:
        discarded_sets = {frozenset(item) for item in discarded}
        frequent_sets = {frozenset(item) for item in set(frequent_itemsets)}

        to_remove = {item for item in frequent_sets if any(subset in discarded_sets for subset in item)}

        frequent_itemsets = set(frequent_itemsets) - to_remove

        candidate_itemsets = generate_candidate_itemsets(frequent_itemsets, k + 1)
        if not candidate_itemsets or k >= 3:
            break

        discarded = set(item_counts.keys()) - set(frequent_itemsets)

        item_counts = {}
        for transaction in data:
            for pair in candidate_itemsets:
                if all(item in transaction for item in pair):
                    item_counts[pair] = item_counts.get(pair, 0) + 1

        for item, count in item_counts.items():
            if count >= min_support and len(item) > 1:
                item_str = ' '.join(item)
                sets_for_association_rules[item] = count

        k += 1

    association_rules_string = ""

    for itemset, count in sets_for_association_rules.items():
        for i in range(1, len(itemset)):
            for subset in itertools.combinations(itemset, i):
                remaining_items = tuple(item for item in itemset if item not in subset)
                remaining_items_str = ','.join(sorted(remaining_items))
                subset_str = ','.join(sorted(subset))
                association_rules_string += f"{subset_str}=>{remaining_items_str}\n"

    rules = []
    for line in association_rules_string.split('\n'):
        if line:
            sum = 0
            first, second = line.split('=>')
            rules.append({first: second})

    values = []
    confidence = {}
    union = 0
    union_list = []

    for association_dict in rules:
        for key, value in association_dict.items():
            key_elements = [item.strip() for item in key.split(",")]
            value_elements = [item.strip() for item in value.split(",")]
            for i in key_elements:
                union_list.append(i)
            for j in value_elements:
                union_list.append(j)
            union_tuple = tuple(sorted(union_list))
            if union_tuple in item_sets_count:
                union = item_sets_count[union_tuple]
            if isinstance(key, str):
                key_tuple = (key,)
            else:
                key_tuple = tuple(key)

            if key_tuple in item_sets_count:
                left = item_sets_count[key_tuple]
            result = union / left

            confidence[f"{key}=>{value}"] = result

        union_list = []

    values = []
    for items in rules:
        for key, value in items.items():
            values.append(value)

    freq_count = {}
    processed_tuples = set()
    for i in values:
        my_tuple = tuple(i.split(","))
        if my_tuple not in processed_tuples:
            for basket in data:
                if set(my_tuple).issubset(basket):
                    if my_tuple not in freq_count:
                        freq_count[my_tuple] = 1
                    else:
                        freq_count[my_tuple] += 1
            processed_tuples.add(my_tuple)

    transactions = [transaction for transaction in data if transaction]
    total = len(transactions)

    interesting_rules = []
    count = 0
    for (key, value), i in zip(confidence.items(), values):
        i = my_tuple = tuple(i.split(","))
        if i in freq_count:
            count += 1
            freq = freq_count[i]
            interest = freq / total
            if interest >= 0.5:
                interesting_rules.append(key)
    unique_id = str(uuid.uuid4())  # Convert UUID to string
    chunk_data = {
        'rule': unique_id,  # Use the string representation of UUID as the rule
        'confidence': interesting_rules  # Use the generated key name
    }
    collection.insert_one(chunk_data)
            

    # Convert sets to lists in chunk_data
    #chunk_data = {
     #   'interesting_rules': interesting_rules
    #}
    
    

    # Insert chunk data into MongoDB
    #collection.insert_one(chunk_data)

    print('Received and saved locally:', data)

