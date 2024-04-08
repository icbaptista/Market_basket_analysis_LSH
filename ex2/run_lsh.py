import pandas as pd
import numpy as np 
from plot import plot_lsh
from lsh import *
from itertools import combinations
import argparse
import math
from sklearn.metrics.pairwise import cosine_similarity

 

# Shingling 
def build_shingles(sentence: str, k: int):
    shingles = []
    for i in range(len(sentence) - k):
        shingles.append(sentence[i:i+k])
    return set(shingles)

def build_vocab(shingle_sets: list):
    # convert list of shingle sets into single set
    full_set = {item for set_ in shingle_sets for item in set_}
    vocab = {}
    for i, shingle in enumerate(list(full_set)):
        vocab[shingle] = i
    return vocab

def one_hot(shingles: set, vocab: dict):
    vector = np.zeros(len(vocab))
    for shingle in shingles:
        if shingle in vocab:
            vector[vocab[shingle]] = 1
        elif shingle == 'special':
            vector[-1] = 1  # Use the last entry for the special element
    return vector

 

# Min Hashing 
def minhash_arr(vocab: dict, resolution: int):
    length = len(vocab.keys())
    arr = np.zeros((resolution, length))
    for i in range(resolution):
        permutation = np.random.permutation(len(vocab)) + 1
        arr[i, :] = permutation.copy() 
    return arr.astype(int)

def get_signature(minhash, vector):
    # get index locations of every 1 value in vector
    idx = np.nonzero(vector)[0].tolist()
    # use index locations to pull only +ve positions in minhash
    shingles = minhash[:, idx]
    
    # find minimum 3 in each hash vector 
    signature = np.min(shingles, axis=1) 
    return signature
 

# Evaluation

# Function to calculate Jaccard similarity
def jaccard_similarity(set1, set2):
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0
    
def probability(s, r, b):
        # s: similarity
        # r: rows (per band)
        # b: number of bands
        return 1 - (1 - s**r)**b
 
def tune_parameters(b_max, r_max, s1=0.80, s2=0.60, target_recall=0.90, target_precision=0.05, n_hashes = 200): 
    
    best_b = 0
    best_r = 0
    best_recall = 0
    best_precision = 1

    for r in range(2, r_max + 1):
        for b in range(1, b_max + 1):
        
            recall = probability(s1, r, b)
            precision = 1 - probability(s2, r, b)

            if recall >= target_recall and precision <= target_precision:
                if recall > best_recall or (recall == best_recall and precision < best_precision) and n_hashes%b ==0:
                    best_b = b
                    best_r = r
                    best_recall = recall
                    best_precision = precision

    return best_b, best_r
 
def calculate_jaccard_similarities(dataset):
    similarities = {}
    for i in range(len(dataset)):
        for j in range(i + 1, len(dataset)):
            set1 = set(dataset[i])
            set2 = set(dataset[j])
            if len(set1) > 0 and len(set2) > 0:  # Ensure the sets are not empty
                intersection = len(set1 & set2)
                union = len(set1 | set2)
                similarity = intersection / union
                similarities[(i, j)] = similarity
    return similarities

def calculate_false_positives_and_negatives(jaccard_similarities, candidate_pairs):
    false_positives = []
    false_negatives = []

    # Iterate over the candidate pairs
    for pair in candidate_pairs:
        # If the pair is not in the Jaccard similarities, it's a false positive
        if pair not in jaccard_similarities:
            false_positives.append(pair)

    # Iterate over the Jaccard similarities
    for pair in jaccard_similarities:
        # If the pair is not in the candidate pairs, it's a false negative
        if pair not in candidate_pairs:
            false_negatives.append(pair)

    return false_positives, false_negatives
 
def evaluate_lsh(b, r, articles, signatures):
    # Sample a subset of your dataset
    dataset_sample = articles[:1000] 

    # Calculate the Jaccard similarities for each pair of data points in the sample
    jaccard_similarities = calculate_jaccard_similarities(dataset_sample)
 
    lsh = LSH(b, r)
    for signature in signatures:
        lsh.add_hash(signature)
    candidate_pairs = lsh.check_candidates()

    # Compare the candidate pairs to the actual similar pairs
    false_positives, false_negatives = calculate_false_positives_and_negatives(jaccard_similarities, candidate_pairs)

    # Calculate the percentages of false positives and false negatives
    false_positive_percentage = len(false_positives) / len(candidate_pairs)
    false_negative_percentage = len(false_negatives) / len(jaccard_similarities)

    print("False positive percentage:", false_positive_percentage)
    print("False negative percentage:", false_negative_percentage)


def run_single_lsh(b, r): 
    
    total_lines = 1000
    
    total_pairs = math.comb(total_lines, 2) 
    
    n_hashes = 200  
    
    
    # Getting Data 
    
    filepath = "covid_news_small.json"

    data = pd.read_json(filepath, lines=True)
    data = data["text"].to_list() 
    articles = data[0:total_lines]  

   
   
    # Shingling
   
    k = 8  # shingle size
 
    #print("Building Shingles...")
   
    # build shingles
    shingles = []
    for sentence in articles:
        shingles.append(build_shingles(sentence, k))
        
    #print("Building Vocab...")
    # build vocab
    vocab = build_vocab(shingles)

    #print("One-Hot Encoding...")
    # one-hot encode our shingles
    shingles_1hot = []
    for shingle_set in shingles:
        if not any(shingle_set):
            shingle_set.add('special')  # Add a special element
        shingles_1hot.append(one_hot(shingle_set, vocab))
    # stack into single numpy array
    shingles_1hot = np.stack(shingles_1hot) 



    # Min Hashing

    #print("Min Hashing...")    

    arr = minhash_arr(vocab, n_hashes)

    signatures = []

    for vector in shingles_1hot:
        signatures.append(get_signature(arr, vector))

    # merge signatures into single array
    signatures = np.stack(signatures) 



    # LSH 

    #print("Locality-Sensitive Hashing (LSH)...")

   

    lsh = LSH(b, r)

    for signature in signatures:
        lsh.add_hash(signature)
        
        
    candidate_pairs = lsh.check_candidates() 
 
    # Function to calculate Jaccard similarity
    def jaccard_similarity(set1, set2):
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        return intersection / union if union != 0 else 0
    
    

    # Ploting

    #print("Plotting...")

    #plot_lsh(signatures, shingles_1hot, candidate_pairs, b, r, total_pairs) 
 
    
    return candidate_pairs, articles
    
def get_results(candidate_pairs, articles):
    
    # Test Values
    
    cond_1= False
    cond_2= False
    
    similarities = []
    cond_1_v = 0
    cond_2_v = 0
    
    for pair in candidate_pairs:
        x, y = pair  
        
        vector_x = articles[x]
        vector_y = articles[y]    
        sim = jaccard_similarity(set(vector_x), set(vector_y))
        similarities.append(sim)

        if sim > 0.85:
            cond_1_v += 1
        
        if sim < 0.6:
            cond_2_v += 1
        
        
    cond_1_res = cond_1_v/len(candidate_pairs)
    cond_2_res = cond_2_v/len(candidate_pairs)

    print("")
    print("Percentage of pairs with more than 85%","similarity: ", "%.3f" % (cond_1_res*100), "%")
    print("Percentage of pairs with less than 60%","similarity: ", "%.3f" % (cond_2_res*100), "%\n") 
    
    if cond_1_res > 0.90:
        cond_1 = True
    
    if cond_2_res < 0.05:
        cond_2 = True
     
    return cond_1, cond_2

if __name__ == '__main__':
    
    # Create the parser 
    
    parser = argparse.ArgumentParser(description='Process some integers.')

    
    
    # Add the arguments
    
    parser.add_argument('-b', type=int, required=True, help='The value for b')
    parser.add_argument('-r', type=int, required=True, help='The value for r')

    
    
    # Parse the arguments
    
    args = parser.parse_args()
    
    b = args.b 
    r = args.r
    
    # Run LSH
    
    #candidate_pairs, articles = run_single_lsh(b, r) 
    
    #cond_1, cond_2 = get_results(candidate_pairs, articles) 
    
    #print("Condition 1:", cond_1)
    #print("Condition 2:", cond_2)
    
    values = ((20, 5), (20, 10), (50, 10), (50,  20), (100,  10),  (200,  10))
    
    for v in values:
        print("\n\n --------------------------------------------------")
        print("\nfor values: ", v)
        
        b, r = v  
 
        candidate_pairs, articles = run_single_lsh(b, r) 
    
        cond_1, cond_2 = get_results(candidate_pairs, articles) 

        
        
        print("Finds as candidates at least 90% of pairs with 85% similarity:", cond_1)
        print("Finds as candidates less than 5% of pairs with 60% similarity:", cond_2)
        
        print("\n")
     

        # Calculate stuff
        false_positives = 0
        false_negatives = 0
        i = 0
        for (idx1, article1), (idx2, article2) in combinations(enumerate(articles), 2):
            i+=1
            similarity = jaccard_similarity(set(article1), set(article2))
            
            if similarity > 0.85 and (idx1, idx2) not in candidate_pairs:
                false_negatives += 1    
            elif similarity < 0.6 and (idx1, idx2) in candidate_pairs:
                false_positives += 1 
                 
            
                
        # Calculate percentages
        total_possitive = len(candidate_pairs)
        total = i
        
        percentage_false_positives = (false_positives / total) * 100
        percentage_false_negatives = (false_negatives / total) * 100

        print("Percentage of false positives:", "%.3f" %  percentage_false_positives, "%")
        print("Percentage of false negatives:", "%.3f" %  percentage_false_negatives, "%")
        
     
    
    