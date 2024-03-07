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
    # Only keep columns PATIENT and CODE
    logData = logData.map(lambda x: (x[2], x[4]))

    # Show the data
    print(logData.take(5))

    # Reorder the data by the patient - column Patient
    logData = logData.sortBy(lambda x: x[0]) 

    # Get the frequencies of the elements in the column Code
    freq = logData.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)

    # Discard elements from column code with frequency less than 500 from logData
    logData = logData.filter(lambda x: freq[x[1]] > 500)

