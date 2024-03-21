from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import sys

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: script.py <input_file> <output_directory>")
        exit(1)

    sc = SparkContext(appName="FrequentItems")

    sc_sql = SparkSession.builder \
        .appName("Example") \
        .getOrCreate()

    sqlContext = SQLContext(sc_sql)

    logData = sc_sql.read.option("header", "true").csv(sys.argv[1])

    # Count the number of rows
    print("Number of rows: ", logData.count())

    # Transform the data into a RDD
    logData = logData.rdd

    # Eliminate the columns that are not needed - columns START, STOP, ENCOUNTER and DESCRIPTION
    # Only keep columns PATIENT and CODE (of disease)
    logData = logData.map(lambda x: (x[2], x[4])).groupBy(lambda x: x[0])
 
    # Count the number of times a disease appears 
    diseasesCount = logData.map(lambda x: (x[1], 1)).reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], False)

    # Show the data
    print("AAAAAAAAAAAAAAAAAAAAAAAA")
    print(diseasesCount.take(5))

    # Reorder the data by the patient - column Patient
    logData = logData.sortBy(lambda x: x[0]) 

    # Save the data
    logData.saveAsTextFile(sys.argv[2])

    # Use broadcast  
    # frequent singletons need to be accessed by whole the workers 
    # spark accumulators -> shared variables 