import re
from pyspark import SparkConf, SparkContext

def normalizedWords(text):
	return re.compile("\W+", re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("Word Count Better Sorted")
sc = SparkContext(conf = conf)

lines = sc.textFile("./Book.txt")
words = lines.flatMap(normalizedWords)
wordCounts = words.map(lambda x:(x, 1)).reduceByKey(lambda x, y: x+y)
wordCountsSorted = wordCounts.map(lambda (x, y): (y, x)).sortByKey()

results = wordCountsSorted.collect()

for result in results:
	count = str(result[0])
	cleanWord = result[1].encode('ascii', 'ignore')
	if(cleanWord):
		print cleanWord, count

