from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Word Count")
sc = SparkContext(conf = conf)

lines = sc.textFile("./Book.txt")
words = lines.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
	cleanWord = word.encode('ascii', 'ignore')
	if(cleanWord):
		print cleanWord, count