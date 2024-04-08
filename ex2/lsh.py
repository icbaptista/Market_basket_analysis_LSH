import numpy as np 
from itertools import combinations 
 
class LSH:
    buckets = []
    counter = 0
    def __init__(self, b, r):
        self.b = b
        self.r = r
        self.buckets = []
        self.counter = 0
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
            if len(self.buckets) >= i+2:
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