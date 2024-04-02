from pyspark import SparkContext, SparkConf
from datetime import datetime
import re
import sys

def kwords(l, k):
    l = [w for w in l if len(w)>=3]
    for i in range(len(l)-k+1):
        yield (" ".join(l[i:i+k]), 1)


if __name__ == "__main__":
    filepath = "lusiadas.txt"
    resultpath = "biwordcount_result"
    if len(sys.argv) == 3:
        filepath = sys.rgv[1]
        resultpath = sys.argv[2]
        #exit(-1)
        
    conf = SparkConf().set('spark.driver.bindAddress','127.0.0.1')
    sc = SparkContext(appName="SparkIntro_BiwordCount", conf=conf)
    

    textfile = sc.textFile(filepath)
    occurences = textfile.map(lambda line: re.split(r'[^\w]+', line.lower())) \
                            .flatMap(lambda l: kwords(l, 2)) \
                            .reduceByKey(lambda a,b: a+b) \
                            .sortBy(lambda p: p[1], False)
    
    print(occurences.take(10))

    #format_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    #occurences.saveAsTextFile("{0}/{1}".format(resultpath, format_time))
    sc.stop()
