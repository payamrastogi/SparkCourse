from pyspark import SparkContext, SparkConf

conf = SparkContext().setMaster("local").setAppName("Degree of Separation")
sc = SparkContext(conf=conf)


