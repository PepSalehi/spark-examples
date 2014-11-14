import re
import sys

from pyspark import SparkContext

#function to extract the data from the line
#based on position and filter out the invalid records
def extractData(line):
    val = line.strip()
    (year, temp, q) = (val[15:19], val[87:92], val[92:93])
    if (temp != "+9999" and re.match("[01459]", q)):
	return [(year, temp)]
    else:
        return []

logFile = "hdfs://localhost:9000/user/bigdatavm/input"

#Create Spark Context with the master details and the application name
sc = SparkContext("spark://localhost:7077", "max_temperature")

#Create an RDD from the input data in HDFS
weatherData = sc.textFile(logFile)

#Transform the data to extract/filter and then find the max temperature
max_temperature_per_year = weatherData.flatMap(extractData).reduceByKey(lambda a,b : a if int(a) > int(b) else b)

#Save the RDD back into HDFS
max_temperature_per_year.saveAsTextFile("hdfs://localhost:9000/user/bigdatavm/output")
