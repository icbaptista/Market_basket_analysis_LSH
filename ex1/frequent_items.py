from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from itertools import combinations
import sys

# Global Spark variables
spark_context = SparkContext(appName="FrequentItems")
spark_session = SparkSession.builder.appName("Example").getOrCreate()
sql_context = SQLContext(spark_session)

class A_Priori:
    def __init__(self, input_file, output_directory, min_support=0.1):
        self.input_file = input_file
        self.output_directory = output_directory
        self.min_support = min_support
        self.conditions_rdd = None
        self.patient_diseases_baskets = None
        self.frequent_itemsets = []
        self.k = 1

    def fetch_data(self):
        """Load the 'conditions.csv.gz' file into a Spark RDD."""
        self.conditions_rdd = spark_session.read.option("header", "true").csv(self.input_file).rdd

    def organize_data(self):
        """Preprocess the data by creating a list of disease codes for each patient."""
        # Reorganize the data to have all the disease codes for each patient in the same line
        self.patient_diseases_baskets = self.conditions_rdd.groupBy(lambda row: row.PATIENT) \
                                                       .mapValues(lambda codes: ", ".join(set(row.CODE for row in codes))) \
                                                       .values() \
                                                       .collect()

    def generate_frequent_1_itemsets(self):
        """Generate frequent 1-itemsets (k = 1)."""
        frequent_1_itemsets = set()
        for patient_codes in self.patient_diseases_baskets:
            for code in patient_codes.split(", "):
                frequent_1_itemsets.add(code)

        self.frequent_1_itemsets = list(frequent_1_itemsets)

    def generate_candidate_itemsets(self, frequent_itemsets, k):
        """Generate candidate itemsets of size k from frequent itemsets of size k-1."""
        candidates = []
        for itemset in frequent_itemsets:
            itemset = itemset.split(", ")
            for item in itemset:
                new_itemset = itemset.copy()
                new_itemset.append(item)
                new_itemset = sorted(set(new_itemset))
                if len(new_itemset) == k:
                    candidates.append(", ".join(new_itemset))
        return candidates

    def calculate_support(self, data, candidates):
        """Count the support of each candidate itemset."""
        support_counts = {}
        for patient_codes in data: # iterating through the patient codes - baskets 
            patient_codes = patient_codes.split(", ")
            for candidate in candidates:
                candidate_codes = candidate.split(", ")
                if all(code in patient_codes for code in candidate_codes):
                    support_counts[candidate] = support_counts.get(candidate, 0) + 1

        return [(k, v) for k, v in support_counts.items() if v >= self.min_support * len(data)]

    def execute_a_priori(self):
        """Run the A-Priori algorithm to find frequent itemsets."""
        self.fetch_data()
        self.organize_data()
        self.generate_frequent_1_itemsets()

        # Run the A-Priori algorithm
        self.frequent_itemsets = self.frequent_1_itemsets
        while self.frequent_itemsets:
            candidate_itemsets = self.generate_candidate_itemsets(self.frequent_itemsets, self.k)
            support_counts = self.calculate_support(self.patient_diseases_baskets, candidate_itemsets)
            self.frequent_itemsets = [item[0] for item in support_counts]
            self.k += 1

        # Print the frequent itemsets
        for itemset in self.frequent_itemsets:
            print(f"Frequent itemset: {itemset}")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_file> <output_directory>")
        print("Where:")
        print("  <input_file>: The path to the 'conditions.csv.gz' file.")
        print("  <output_directory>: The path to the output directory where the results will be saved.")
        exit(1)

    a_priori = A_Priori(sys.argv[1], sys.argv[2])
    a_priori.execute_a_priori()
