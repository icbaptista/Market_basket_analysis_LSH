import pandas as pd
import numpy as np 
from lsh import *
from itertools import combinations

import argparse

# Create the parser
parser = argparse.ArgumentParser(description='Process some integers.')

# Add the arguments
parser.add_argument('-b', type=int, required=True, help='The value for b')
parser.add_argument('-r', type=int, required=True, help='The value for r')

# Parse the arguments
args = parser.parse_args()
 
n_hashes = 200 
 
# Getting Data 

filepath = "covid_news_small.json"

data = pd.read_json(filepath, lines=True)
data = data["text"].to_list() 
articles = data[0:1000]  

# Shingling

k = 8  # shingle size

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


print("Building Shingles...")
# build shingles
shingles = []
for sentence in articles:
    shingles.append(build_shingles(sentence, k))
    
print("Building Vocab...")
# build vocab
vocab = build_vocab(shingles)

print("One-Hot Encoding...")
# one-hot encode our shingles
shingles_1hot = []
for shingle_set in shingles:
    if not any(shingle_set):
        shingle_set.add('special')  # Add a special element
    shingles_1hot.append(one_hot(shingle_set, vocab))
# stack into single numpy array
shingles_1hot = np.stack(shingles_1hot) 



# Min Hashing
print("Min Hashing...")
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

arr = minhash_arr(vocab, n_hashes)

signatures = []

for vector in shingles_1hot:
    signatures.append(get_signature(arr, vector))

# merge signatures into single array
signatures = np.stack(signatures) 


# LSH 

print("Locality-Sensitive Hashing (LSH)...")

b = args.b 
r = args.r

lsh = LSH(b, r)

for signature in signatures:
    lsh.add_hash(signature)
     
    
candidate_pairs = lsh.check_candidates() 

# Ploting

print("Plotting...")

from sklearn.metrics.pairwise import cosine_similarity
import matplotlib.pyplot as plt 
 
 

def jaccard(a: set, b: set):
    return len(a.intersection(b)) / len(a.union(b))

pairs = pd.DataFrame({
    'x': [],
    'y': [],
    'jaccard': [],
    'cosine': [],
    'candidate': []
})

x_= []
y_= []
jaccard_ = []
cosine_ = []
candidate_ = [] 

data_len = shingles_1hot.shape[0]
chosen = set()
# take random sample of pairs
sample_size = 50_000
for _ in range(sample_size):
    
    x, y = np.random.choice(data_len, 2)
    if x == y or (x, y) in chosen: continue
    chosen.add((x, y))
    
    vector_x = signatures[x]
    vector_y = signatures[y]    
    candidate = 1 if (x, y) in candidate_pairs else 0
    cosine = cosine_similarity([vector_x], [vector_y])[0][0]
    
    x_.append(x)
    y_.append(y)
    jaccard_.append(jaccard(set(vector_x), set(vector_y)))
    cosine_.append(cosine)
    candidate_.append(candidate)

 

# add a normalized cosine column for better alignment
cos_min = min(cosine_)
cos_max = max(cosine_)   
 
cosine_norm = (cosine_ - cos_min) / (cos_max - cos_min)

def probability(s, r, b):
    # s: similarity
    # r: rows (per band)
    # b: number of bands
    return 1 - (1 - s**r)**b

def normalize(x, x_min, x_max):
    return (x - x_min) / (x_max - x_min)
 
r = int(100 / b)
s_scores = np.arange(0.01, 1, 0.01)
P_scores = [probability(s, r, b) for s in s_scores]

plt.plot(s_scores, P_scores)
plt.scatter(x=cosine_norm, y=candidate_, alpha=0.5)
plt.ylabel('candidates')
plt.xlabel('Similarity (cosine)')
plt.show()

def tune_parameters(b_max, r_max, s1=0.80, s2=0.60, target_recall=0.90, target_precision=0.05):
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

#b, r = tune_parameters(300, 1000)

print("Best b:", b)
print("Best r:", r)
 
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
 
def evaluate_lsh(b, r):
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

values = [(20, 10)]

for v in values:
    evaluate_lsh(v[0], v[1])