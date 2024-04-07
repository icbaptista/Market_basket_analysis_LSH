# Use broadcast  

- frequent singletons need to be accessed by whole the workers 
- spark accumulators -> shared variables 

- Second pass - Count the number of times a disease appears (k = 2)
- Generate all combinations of diseases (only consider frequent items)
- frequentDiseases = diseasesCount.map(lambda x: x[0]).collect()
- combinations = conditions_rdd.flatMap(lambda x: combinations(x[1], 2)).filter(lambda x: all(item in frequentDiseases for item in x))

```
def generate_frequent_1_itemsets(self):
        """Generate frequent 1-itemsets (k = 1)."""
        frequent_1_itemsets_rdd = self.conditions_rdd.map(lambda row: (row.CODE, 1)) \
                                                    .reduceByKey(lambda a, b: a + b) \
                                                    .filter(lambda x: x[1] >= self.min_support * self.conditions_rdd.count()) \
                                                    .map(lambda x: x[0])
        
        self.frequent_1_itemsets = frequent_1_itemsets_rdd.collect()
```

```
def calculate_support(self, data, candidates):
    """Count the support of each candidate itemset using Spark's distributed processing."""
    candidate_broadcast = spark_context.broadcast(candidates)  # Broadcast candidates to all nodes

    def count_support(iterator):
        support_counts = {}
        for patient_codes in iterator:
            for candidate in candidate_broadcast.value:
                candidate_codes = candidate.split(", ")
                if all(code in patient_codes for code in candidate_codes):
                    support_counts[candidate] = support_counts.get(candidate, 0) + 1
        return support_counts.items()

    frequent_itemsets = data.mapPartitions(count_support) \
                            .reduceByKey(lambda a, b: a + b) \
                            .filter(lambda x: x[1] >= self.support_threshold) \
                            .collect()

    print(f"\n\n Frequent itemsets for k={self.k} (first 10 items):")
    print(frequent_itemsets[:10])
    return frequent_itemsets
```

  def generate_candidates(self, k, frequent_itemsets): 
        """Generate candidate itemsets of size k from frequent itemsets of size k-1 using itertools.combinations."""
        candidate_itemsets = []
        for itemset in frequent_itemsets[k-1]:
            itemset_list = itemset.split(", ")
            for candidate_tuple in combinations(itemset_list, k):
                candidate = ", ".join(sorted(candidate_tuple))
                candidate_itemsets.append(candidate)

        # Filter candidates based on minimum support threshold
        candidate_itemsets = [candidate for candidate in candidate_itemsets if self.calculate_support([candidate], frequent_itemsets[k-1]) >= self.support_threshold]

        print(f"\n\n Candidate itemsets for k={k} (first 10 items):")
        print(candidate_itemsets[:10])

        return candidate_itemsets

        # Calculate frequent itemsets for k = 1
        frequent_items_k1 = patient_diseases_baskets.flatMap(lambda line: line.split(", ")) \
                                                    .map(lambda condition: (condition, 1)) \
                                                    .reduceByKey(lambda a, b: a + b) \
                                                    .filter(lambda entry: entry[1] >= self.support_threshold) \
                                                    .keys() \
                                                    .collect()
        print(f"\n\n Frequent itemsets (k = 1)\n")
        print(list(frequent_items_temp.items())[:10])
        print("\n\n")


        def calculate_support(self, candidates):
        """Count the support of each candidate itemset."""
        support_counts = {}
        for patient_codes in my_casted_itemsets.value[1]: # iterating through the patient codes - baskets 
            patient_codes = patient_codes.split(", ")
            for candidate in candidates:
                candidate_codes = candidate.split(", ")
                if all(code in patient_codes for code in candidate_codes):
                    support_counts[candidate] = support_counts.get(candidate, 0) + 1

        frequent_itemsets_support = [(k, v) for k, v in support_counts.items() if v >= self.support_threshold]
        print(f"\n\n Frequent itemsets for k={self.k} (first 10 items):")
        print(frequent_itemsets_support[:10])
        return frequent_itemsets_support

         def generate_candidates(self, k):
        """Generate candidate itemsets of size k from frequent itemsets of size k-1 using itertools.combinations."""
        candidate_itemsets = []
        candidate_set = set()
        for itemset in my_casted_itemsets.value[1]:
            itemset_list = itemset.split(", ")
            for candidate_tuple in combinations(itemset_list, k):
                candidate = ", ".join(sorted(candidate_tuple))
                candidate_set.add(candidate)
                if candidate not in candidate_set:
                    candidate_itemsets.append((candidate, 1))
                else:

                    candidate_itemsets.append((candidate, count))

        print(f"\n\n Candidate itemsets for k={k} (first 10 items):")
        print(candidate_itemsets[:10])
        return candidate_itemsets


        # Generate candidate itemsets for k > 1
        candidate_itemsets = baskets.flatMap(lambda line: combinations(line.split(", "), k)) \
                                .map(lambda candidate: (", ".join(sorted(candidate)), 1)) \
                                .reduceByKey(lambda a, b: a + b) \
                                .filter(lambda x: x[1] >= 10) 
                            
        # Filter frequent itemsets where inidividual items are frequent
        freq_candidates = candidate_itemsets.filter(lambda combinations: all(item in freq_items_broadcast.value[k-1] for item in combinations)) 



candidate_itemsets = baskets \
            .flatMap(lambda line: [tuple(sorted(list(c)) for c in combinations(line.split(", "), k) if all(item in freq_items_broadcast.value[k-1] for item in c))]) \
            .map(lambda candidate: (candidate, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .filter(lambda x: x[1] >= 10) \
            .collectAsMap()
