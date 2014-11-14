import re
import sys

from pyspark import SparkContext

#Create Spark Context with the master details and the application name
sc = SparkContext("spark://bigdata-vm:7077", "max_temperature")

#Add a file to be downloaded with this Spark job on every node.
sc.addFile("/home/bigdatavm/Code/Spark/filter_weather_records.rb")

#Create an RDD from the input data in HDFS
weatherData = sc.textFile("hdfs://localhost:9000/user/bigdatavm/input")

#Transform the data to extract/filter and then find the max temperature
max_temperature_per_year = weatherData.pipe("filter_weather_records.rb").map(lambda x: (x.split("\t")[0], x.split("\t")[1])).reduceByKey(lambda a,b : a if int(a) > int(b) else b).coalesce(1)

#Save the RDD back into HDFS
max_temperature_per_year.saveAsTextFile("hdfs://localhost:9000/user/bigdatavm/output")
