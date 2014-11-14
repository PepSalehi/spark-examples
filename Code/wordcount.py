from pyspark import SparkContext, SparkConf

logFile = "hdfs://localhost:9000/user/bigdatavm/input"

conf = (SparkConf().set("num-executors", "2"))

sc = SparkContext(master = "spark://localhost:7077", appName = "WordCount", conf = conf)

textFile = sc.textFile(logFile)

wordCounts = textFile.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)

wordCounts.saveAsTextFile("hdfs://localhost:9000/user/bigdatavm/output")
