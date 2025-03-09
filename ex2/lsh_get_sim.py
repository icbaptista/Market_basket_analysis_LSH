import pandas as pd
import numpy as np 

from itertools import combinations

import argparse

# Create the parser
parser = argparse.ArgumentParser(description='Process some integers.')

# Add the arguments
parser.add_argument('-b', type=int, required=True, help='The value for b')
parser.add_argument('-r', type=int, required=True, help='The value for r')

# Parse the arguments
args = parser.parse_args()
 
class LSH:
    buckets = []
    counter = 0
    def __init__(self, b, r):
        self.b = b
        self.r = r
        for i in range(b):
            self.buckets.append({})

    def make_subvecs(self, signature):
        l = len(signature) 
        assert l % self.b == 0
        r = self.r
        # break signature into subvectors
        subvecs = []
        for i in range(0, l, r):
            subvecs.append(signature[i:i+r])
        return np.stack(subvecs)
    
    def add_hash(self, signature):
        subvecs = self.make_subvecs(signature).astype(str)
        for i, subvec in enumerate(subvecs):
            subvec = ','.join(subvec)
            if subvec not in self.buckets[i].keys():
                self.buckets[i][subvec] = []
            self.buckets[i][subvec].append(self.counter)
        self.counter += 1

    def check_candidates(self):
        candidates = []
        for bucket_band in self.buckets:
            keys = bucket_band.keys()
            for bucket in keys:
                hits = bucket_band[bucket]
                if len(hits) > 1:
                    candidates.extend(combinations(hits, 2))
        return set(candidates)

# Getting Data 

filepath = "covid_news_small.json"

data = pd.read_json(filepath, lines=True)
data = data["text"].to_list() 
articles = data[0:1000] 

import requests
import pandas as pd
import numpy as np
import io

url = "https://raw.githubusercontent.com/brmson/dataset-sts/master/data/sts/sick2014/SICK_train.txt"

text = requests.get(url).text

data = pd.read_csv(io.StringIO(text), sep='\t')
data.head()

sentences = data['sentence_A'].tolist() 

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

arr = minhash_arr(vocab, 200)

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

print("Getting the one...")



# Function to calculate Jaccard similarity
def jaccard_similarity(set1, set2):
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0
 
 
def find_similar_articles(article):
    article_shingles = build_shingles(article, k)
    similar_articles = []

    for pair in candidate_pairs:
        x, y = pair 
        if article == articles[x] or article == articles[y]:
            
            other_article = articles[y] if article == articles[x] else articles[x]
            
            vector_x = articles[x]
            vector_y = articles[y]    
            
            sim = jaccard_similarity(set(vector_x), set(vector_y)) 

            if sim < 1.0 and sim > 0.85:
                similar_articles.append(other_article)
    return similar_articles

print("\n\n")
for i in range(0, 100):
    results = find_similar_articles(articles[i])
    if len(results) > 0:
        print("article being searched: ", articles[i])
        print("\n\n")
        print("similar articles found: ")
        print("\n")
        for result in results: 
            print("-> ", result)
            print("")
        print("\n\n")
 