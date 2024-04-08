from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from itertools import combinations
from fractions import Fraction
import sys
import os

# Global Spark variables
spark_context = SparkContext(appName="FrequentItems")
spark_session = SparkSession.builder.appName("Example").getOrCreate()
sql_context = SQLContext(spark_session)

frequent_itemsets = {}

class A_Priori:
    def __init__(self, input_file, output_directory):
        self.input_file = input_file
        self.output_directory = output_directory
        self.support_threshold = 1000
        self.conditions_rdd = None
        self.k = 1

    def fetch_data(self, percentage=5):
        """Load a specified percentage of the 'conditions.csv.gz' file into a Spark RDD."""
        full_rdd = spark_session.read.option("header", "true").csv(self.input_file).rdd
        total_count = full_rdd.count()
        sample_rdd = full_rdd.sample(False, percentage / 100.0, seed=42)
        self.conditions_rdd = sample_rdd
        print(f"\n\n Loaded {percentage}% of the data ({sample_rdd.count()} out of {total_count} rows)")
        print(self.conditions_rdd.take(5))

    def calculate_lift(self, k, frequent_items_k1, confidence):
        """Calculate the lift of each confident candidate itemset in the data."""
        frequent_items_k1 = freq_items_broadcast.value[1]
        
        lift = {}
        for candidate, conf in confidence.items():
            antecedent = candidate.difference(frequent_items_k1)
            consequent = candidate.intersection(frequent_items_k1)
            
            support_antecedent = frequent_items_k1.get(antecedent.pop(), 0)
            support_consequent = frequent_items_k1.get(consequent.pop(), 0)
            
            lift[candidate] = (conf / (support_antecedent * support_consequent + 1e-10))

        return lift

    def calculate_standardized_lift(self, lift, confidence):
        """Calculate the standardized lift of each confident candidate itemset."""
        standardized_lift = {}
        for candidate, l in lift.items():
            if candidate in confidence and confidence[candidate] != 0:
                standardized_lift[candidate] = ((l - 1) / (confidence[candidate] - 1)) ** 0.5
            else:
                standardized_lift[candidate] = 0
                
        return standardized_lift

    
def is_valid_candidate(comb, k):
    """ Check if a combination is a valid candidate by checking if all its subsequences of length 1 to k-1 are frequent."""
    # Check if all subsequences of length 1 to k-1 are frequent
    for i in range(1, k):
        subsequences = combinations(comb, i) # Generate all subsequences of length i from the combination
        if not all(is_frequent_subsequence(sorted(list(x)), i) for x in subsequences): # Check if all subsequences of length i are frequent
            return False
    return True

def is_frequent_subsequence(comb, k):
    """ Check if a subsequence of length k is frequent."""
    if k == 1: # For k=1, check if the first element of the subsequence is in the frequent items table
        return comb[0] in freq_items_broadcast.value[k]
    return tuple(comb) in freq_items_broadcast.value[k] # For k>1, check if the subsequence is in the frequent items table


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_file> <output_directory>")
        print("Where:")
        print("  <input_file>: The path to the 'conditions.csv.gz' file.")
        print("  <output_directory>: The path to the output directory where the results will be saved.")
        exit(1)

    a_priori = A_Priori(sys.argv[1], sys.argv[2])
        
    print("\n\n Starting A-Priori Algorithm \n\n")
    a_priori.fetch_data(percentage=2) 
    
    print("\n\n Data Fetched \n\n")
    """Preprocess the data by creating a list of disease codes for each patient."""
    patient_diseases_baskets = a_priori.conditions_rdd.groupBy(lambda row: row.PATIENT) \
                                                    .mapValues(lambda codes: ", ".join(set(row.CODE for row in codes))) \
                                                    .values()
    
    print("\n\n Example of organized data (first 10 entries):")
    print(patient_diseases_baskets.take(10))

    patient_diseases_baskets.saveAsTextFile("baskets.txt")
    print(f"\n\n Organized data saved to: baskets\n")

    baskets = spark_context.textFile("baskets.txt")

    min_support = 1000
    freq_items_broadcast = spark_context.broadcast({})
    
    freq_items_k1 = (baskets.flatMap(lambda line: line.split(", "))
            .map(lambda condition: (condition, 1))
            .reduceByKey(lambda a, b: a + b)
            .filter(lambda entry: entry[1] > min_support)
            .collectAsMap())
    
    print(f"\n\n Candidate itemsets for k=1 (first 10 items):")
    print(list(freq_items_k1.items())[:10])
    print("\n\n")

    freq_items_broadcast.value[1] = freq_items_k1
    freq_items_broadcast = spark_context.broadcast(freq_items_broadcast.value)

    # Create results folder 
    if not os.path.exists(a_priori.output_directory):
        os.makedirs(a_priori.output_directory)

    # Save frequent itemsets for current k to a file
    output_file = f"{a_priori.output_directory}/frequent_itemsets_k_1.txt"
    with open(output_file, "w") as file:
        file.write("Top 10 frequent itemsets for k=1:\n")
        for (itemset, count) in list(freq_items_k1.items())[:10]:
            file.write(f"{itemset}: {count}\n")

    # Run the A-Priori algorithm up to from 2 to max_k
    max_k = 3 
    for k in range(2, max_k+1):
        # Generate candidate itemsets for k > 1
        candidate_itemsets = baskets \
            .flatMap(lambda line: [tuple(sorted(list(c))) for c in combinations(line.split(", "), k) if (is_valid_candidate(c,k))]) \
            .map(lambda combination: (combination, 1)) \
            .reduceByKey(lambda a, b: a + b) \
            .filter(lambda entry: entry[1] > 2) \
            .sortBy(lambda x: x[1], False) \
            .collectAsMap()

        print(f"\n\n Candidate itemsets for k={k} (first 10 items):")
        print(list(candidate_itemsets.items())[:10])
        print("\n\n")

        freq_items_broadcast.value[k] = candidate_itemsets
        freq_items_broadcast = spark_context.broadcast(freq_items_broadcast.value)

        # Save frequent itemsets for current k to a file
        output_file = f"{a_priori.output_directory}/frequent_itemsets_k_{k}.txt"
        with open(output_file, "w") as file:
            file.write(f"Top 10 frequent itemsets for k={k}:\n")
            for (itemset, count) in list(candidate_itemsets.items())[:10]:
                file.write(f"{itemset}: {count}\n")
        
    print("\n\n Frequent Items Generated and saved to file \n\n")

    # Calculate confidence, lift, and standardized lift for each frequent itemset
    freq_items_k2 = list(freq_items_broadcast.value[2].items())
    freq_items_k3 = list(freq_items_broadcast.value[3].items())

    # Create RDDs from the lists
    rdd_k2 = spark_context.parallelize(freq_items_k2)
    rdd_k3 = spark_context.parallelize(freq_items_k3)

    output_file = f"{a_priori.output_directory}/association_rules_k_{2}.txt"
    with open(output_file, "w") as rules_file:
        # Calculate association rules for k=2

        # Support (A,B): transactions containing both itemA and itemB - already calculated 
        
        # Calculate confidence for k=2, considering both directions
        # Confidence (A->B): transactions containing both itemA and itemB / transactions containing itemA
        confidence_k2 = rdd_k2 \
            .flatMap(lambda itemset: [
                ((itemset[0][0], itemset[0][1], freq_items_broadcast.value[2].get(itemset[0], 1) / freq_items_broadcast.value[1].get(itemset[0][0], 1))),
                ((itemset[0][1], itemset[0][0], freq_items_broadcast.value[2].get(itemset[0], 1) / freq_items_broadcast.value[1].get(itemset[0][1], 1)))
            ])

        print("\n\n Calculated Confidence for k=2 \n\n")
        print(confidence_k2.take(10))  
        rules_file.write("confidence_k2\n")
        rules_file.write(str(confidence_k2.take(10)))
        
        # TODO: fix this and apply it for the k = 3 case 
        # Calculate confidence for k=3   
        confidence_k3 = rdd_k2.flatMap(lambda itemset: [  # (A,B,C, Prob) (B,C,A, Prob) (C,A, Prob) (C, Prob)
                ((itemset[0][0], itemset[0][1], freq_items_broadcast.value[2].get(itemset[0], 1) / freq_items_broadcast.value[1].get(itemset[0][0], 1))),
                ((itemset[0][1], itemset[0][0], freq_items_broadcast.value[2].get(itemset[0], 1) / freq_items_broadcast.value[1].get(itemset[0][1], 1)))
            ])
        
        print("\n\n Calculated Confidence for k=3 \n\n")
        print(confidence_k3.take(10)) 

        # Calculate interest for k=2
        # Interest (A->B): confidence(A->B) - support(B) represents how much more likely itemB is purchased when itemA is purchased
        interest_k2 = confidence_k2 \
            .map(lambda rule: (rule[0], rule[1], rule[2] - (freq_items_broadcast.value[1].get(rule[1], 1) / sum(freq_items_broadcast.value[1].values()))))
        
        print("\n\n Calculated Interest for k=2 \n\n")
        print(interest_k2.take(10))

        rules_file.write("\ninterest_k2\n")
        rules_file.write(str(interest_k2.take(10)))

        # Calculate lift for k=2
        # Lift (A->B): confidence(A->B) / support(B) represents how much more likely itemB is purchased when itemA is purchased
        lift_k2 = confidence_k2 \
            .map(lambda rule: (rule[0], rule[1], rule[2] / freq_items_broadcast.value[1].get(rule[0][1], 1)))

        print("\n\n Calculated Lift for k=2 \n\n")
        print(lift_k2.take(10))
        rules_file.write("\nlift_k2\n")
        rules_file.write(str(lift_k2.take(10)))

        # Calculate standardized lift for k=2
        # Standardized Lift (A->B): (lift(A->B) - 1) / (confidence(A->B) - 1) -> normalized lift
        max_lift = lift_k2.map(lambda x: x[2]).max() # maximum lift value

        # Calculate standardized with a minimum threshold of 0.2
        min_standardized_lift = 0.2
        standardized_lift_k2 = lift_k2 \
            .map(lambda x: (x[0], x[1], (x[2] - 1) / (max_lift - 1))) \
            .filter(lambda x: x[2] >= min_standardized_lift) \
            .sortBy(lambda x: x[2], False)
        
        rules_file.write("\nstandardized_lift_k2\n")
        rules_file.write(str(standardized_lift_k2.take(10)))

        print("\n\n Calculated Lift for k=2 \n\n")
        print(standardized_lift_k2.take(10))

        # Write association rules to the text file
        rules_file.write("Association Rules\n")
        rules_file.write("=========================================\n")
        rules_file.write("Association Rule	  Std Lift	Lift  Confidence Interest\n")
        rules_file.write("=========================================\n")

        # Write association rules for k=2
        for rule in standardized_lift_k2.collect():
            association_rule = f"{rule[0]} -> {rule[1]}"
            standardized_lift = rule[2]
            lift = lift_k2.filter(lambda x: x[0] == rule[0] and x[1] == rule[1]).map(lambda x: x[2]).collect()[0]
            confidence = confidence_k2.filter(lambda x: x[0] == rule[0] and x[1] == rule[1]).map(lambda x: x[2]).collect()[0]
            interest = interest_k2.filter(lambda x: x[0] == rule[0] and x[1] == rule[1]).map(lambda x: x[2]).collect()[0]
            rules_file.write(f"{association_rule}\t{standardized_lift:.4f}\t{lift:.4f}\t{confidence:.4f}\t{interest:.4f}\n")

        
    print("\n\n Association Rules Written \n\n")

    spark_session.stop()