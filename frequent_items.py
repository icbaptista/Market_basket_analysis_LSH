from pyspark.sql import SparkSession
import sys 
from pyspark.sql import SQLContext

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print("Usage: script.py <input_file> <output_directory>")
        exit(1)
    
    sc_sql = SparkSession.builder \
        .appName("Example") \
        .getOrCreate()
    
    sqlContext = SQLContext(sc_sql)

    logData = sc_sql.read.option("header", "true").csv(sys.argv[1])

    # Count the number of rows
    print("Number of rows: ", logData.count())

    # Reorder the data by the patient - column Patient
    logData.createOrReplaceTempView("logData")
    logData = sqlContext.sql("SELECT * FROM logData ORDER BY Patient")

    # Show the data
    logData.show()





