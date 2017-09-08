import re
from pyspark import SparkConf, SparkContext

def normalizedWords(text):
	return re.compile("\W+", re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("Word Count Better")
sc = SparkContext(conf = conf)


lines = sc.textFile("./Book.txt")
words = lines.flatMap(normalizedWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
	cleanWord = word.encode('ascii', 'ignore')
	if(cleanWord):
		print cleanWord, count
