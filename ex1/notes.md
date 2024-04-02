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