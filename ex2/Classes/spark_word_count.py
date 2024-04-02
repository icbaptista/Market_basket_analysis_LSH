from pyspark import SparkContext
from datetime import datetime
import re
import sys

if __name__ == "__main__":
    filepath = "lusiadas.txt"
    resultpath = "wordcount_result"
    if len(sys.argv) == 3:
        filepath = sys.rgv[1]
        resultpath = sys.argv[2]
        #exit(-1)
        
    sc = SparkContext(appName="SparkIntro_WordCount")

    textfile = sc.textFile(filepath)
    occurences = textfile.flatMap(lambda line: re.split(r'[^\w]+', line.lower())) \
                            .filter(lambda w: len(w)>3) \
                            .map(lambda word : (word, 1)) \
                            .reduceByKey(lambda a,b: a+b) \
                            .sortBy(lambda p: p[1], False)

    format_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    occurences.saveAsTextFile("{0}/{1}".format(resultpath, format_time))
    sc.stop()
