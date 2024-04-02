
from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF, MinHashLSH 
from functools import reduce
from pyspark.sql.functions import udf, col, count
from pyspark.sql.types import ArrayType, StringType
from pyspark.ml.feature import HashingTF
from pyspark.ml.feature import MinHashLSH
from pyspark.ml.feature import LSHModel 

import sys

if __name__ == '__main__':
 
        
    filepath = "covid_news_small.json"
    resultpath = "results.txt"

    # Create a Spark session

    spark = SparkSession.builder.appName("NewsArticleSimilarity").getOrCreate()

    
    # Load the news articles from a JSON file

    df = spark.read.json(filepath)


    # Shingling

    def get_shingles(text, shingle_size=5):
        """
        Converts a given text into a set of shingles (overlapping substrings).
        """
        shingles = [text[i:i+shingle_size] for i in range(len(text) - shingle_size + 1)]
        return list(set(shingles))

    get_shingles_udf = udf(get_shingles, ArrayType(StringType()))


    # Hashing Shingles

    hashing_tf = HashingTF(inputCol="shingles", outputCol="hashed_shingles", numFeatures=1000)
    df = hashing_tf.transform(df)


    # MinHashing

    minhash = MinHashLSH(inputCol="hashed_shingles", outputCol="minhash_signature", numHashTables=10)
    model = minhash.fit(df)
    df = model.transform(df)


    # Locality-Sensitive Hashing (LSH)

    lsh = LSHModel(inputCol="minhash_signature", outputCol="lsh_buckets", numHashTables=5)
    df = lsh.transform(df)

    bucket_counts = df.groupBy("lsh_buckets").count().orderBy("count", ascending=False)
    bucket_counts.show()