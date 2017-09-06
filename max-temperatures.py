from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Max Temperatures")
sc = SparkContext(conf = conf)

def parseLines(line):
	fields = line.split(",")
	stationId = fields[0]
	entryType = fields[2]
	temp = float(fields[3])/10
	return (stationId, entryType, temp)

lines = sc.textFile("./1800.csv")
rdd = lines.map(parseLines)
maxTemps = rdd.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x, y))
results = maxTemps.collect()

for result in results:
	print result[0] + "\t{:.2f}C".format(result[1])