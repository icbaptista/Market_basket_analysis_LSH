import numpy as np 
from lsh import * 
from sklearn.metrics.pairwise import cosine_similarity
import matplotlib.pyplot as plt 
 
def plot_lsh(signatures, shingles_1hot, candidate_pairs, b, r, total_pairs):
    
    def jaccard(a: set, b: set):
        return len(a.intersection(b)) / (len(a.union(b)))
    
    x_= []
    y_= []
    jaccard_ = []
    cosine_ = []
    candidate_ = [] 

    data_len = shingles_1hot.shape[0]
    chosen = set()
    # take random sample of pairs
    sample_size = total_pairs
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
 
    
    r = int(100 / b)
    s_scores = np.arange(0.01, 1, 0.01)
    P_scores = [probability(s, r, b) for s in s_scores]

    plt.plot(s_scores, P_scores)
    plt.scatter(x=jaccard_, y=candidate_, alpha=0.5)
    plt.ylabel('candidates')
    plt.xlabel('Similarity (cosine)')
    plt.show()