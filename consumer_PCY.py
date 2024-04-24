from kafka import KafkaConsumer
from collections import defaultdict
from itertools import combinations as combo

def create_itemsets(candidates, k):
    return set([i.union(j) for i in candidates for j in candidates if len(i.union(j)) == k])

def hash_bucketing(bucket_num, baskets, threshold):
    buckets = [0] * bucket_num
    for basket in baskets:
        for i, j in combo(basket, 2):
            index = (i + j) % bucket_num
            buckets[index] += 1
    frequent_buckets = set([i for i, v in enumerate(buckets) if v >= threshold])
    return frequent_buckets

def filter_itemsets(itemsets, transactions, min_supp, freq_items, bucket_num, threshold, frequent_buckets):
    candidate_counts = defaultdict(int)
    for item in itemsets:
        for transaction in transactions:
            if item.issubset(transaction):
                candidate_counts[item] += 1

    pruned_itemsets = set()
    local_freq_items = set()
    total_transactions = len(transactions)
    for item, count in candidate_counts.items():
        support = count / total_transactions
        if support >= min_supp:
            pruned_itemsets.add(item)
            local_freq_items.add(item)
            freq_items[item] = support

    pair_counts = defaultdict(int)
    for basket in transactions:
        frequent_items_in_basket = [item for item in basket if item in frequent_buckets]
        for i, j in combo(frequent_items_in_basket, 2):
            if (i, j) in itemsets or (j, i) in itemsets:
                pair_counts[frozenset([i, j])] += 1
    for pair, count in pair_counts.items():
        support = count / total_transactions
        if support >= min_supp:
            pruned_itemsets.add(pair)
            local_freq_items.add(pair)
            freq_items[pair] = support
    return pruned_itemsets, local_freq_items

def pcy_approach(transactions, min_supp):
    freq_items = {}
    candidate_itemsets = set()
    for transaction in transactions:
        for item in transaction:
            candidate_itemsets.add(frozenset([item]))

    bucket_num = 100
    threshold = min_supp * len(transactions)
    frequent_buckets = hash_bucketing(bucket_num, transactions, threshold)
    k = 2
    current_freq_items = set()
    while True:
        current_freq_items, local_freq_items = filter_itemsets(candidate_itemsets, transactions, min_supp, freq_items, bucket_num, threshold, frequent_buckets)
        if len(current_freq_items) == 0:
            break
        freq_items.update(local_freq_items)
        candidate_itemsets = create_itemsets(current_freq_items, k)
        k += 1
    return freq_items

def process_preprocessed_data():
    consumer = KafkaConsumer('preprocessed_data', bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest', group_id=None)

    transactions = []
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        transactions.append(data['categories'])

        frequent_items = pcy_approach(transactions, 0.1)
        print("\nFrequent Itemsets (PCY Algorithm):")
        for itemset, support in frequent_items.items():
            print(f"{itemset}: {support}")

if _name_ == "main":
    process_preprocessed_data()
