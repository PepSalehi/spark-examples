from pyspark import SparkContext


sc = SparkContext("spark://bigdata-vm:7077", "Map")
lines = sc.parallelize(["hello world", "hi"])

wordsWithMap = lines.map(lambda line: line.split(" ")).coalesce(1)
wordsWithFlatMap = lines.flatMap(lambda line: line.split(" ")).coalesce(1)

wordsWithMap.saveAsTextFile("hdfs://localhost:9000/user/bigdatavm/wordsWithMap")
wordsWithFlatMap.saveAsTextFile("hdfs://localhost:9000/user/bigdatavm/wordsWithFlatMap")
