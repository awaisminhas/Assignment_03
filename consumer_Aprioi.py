from kafka import KafkaConsumer
import json
from collections import defaultdict

def find_combinations(itemset, k):
    return set([i.union(j) for i in itemset for j in itemset if len(i.union(j)) == k])

def filter_itemset(itemset, transactions, min_support, freq_items):
    candidate_counts = defaultdict(int)
    for item in itemset:
        for transaction in transactions:
            if item.issubset(transaction):
                candidate_counts[item] += 1

    pruned_itemset = set()
    local_frequent_items = set()
    total_transactions = len(transactions)
    for item, count in candidate_counts.items():
        support = count / total_transactions
        if support >= min_support:
            pruned_itemset.add(item)
            local_frequent_items.add(item)
            freq_items[item] = support
    return pruned_itemset, local_frequent_items

def apriori_algorithm(transactions, min_support):
    freq_items = {}
    candidate_itemset = set()
    for transaction in transactions:
        for item in transaction:
            candidate_itemset.add(frozenset([item]))

    k = 2
    current_frequent_items = set()
    while True:
        current_frequent_items, local_frequent_items = filter_itemset(candidate_itemset, transactions, min_support, freq_items)
        if len(current_frequent_items) == 0:
            break
        freq_items.update(local_frequent_items)
        candidate_itemset = find_combinations(current_frequent_items, k)
        k += 1
    return freq_items

def process_preprocessed_data():
    consumer = KafkaConsumer('preprocessed_data', bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest', group_id=None)

    transactions = []
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        transactions.append(data['categories'])

        frequent_itemsets = apriori_algorithm(transactions, 0.1)
        print("Frequent Itemsets (Apriori Algorithm):")
        for itemset, support in frequent_itemsets.items():
            print(f"{itemset}: {support}")

if _name_ == "main":
    process_preprocessed_data()
