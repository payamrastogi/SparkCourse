from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Min Temperature")
sc = SparkContext(conf=conf)

def parseLine(line):
	fields = line.split(",")
	stationId = fields[0]
	tempType = fields[2]
	temp = float(fields[3])/10
	return (stationId, tempType, temp)

lines = sc.textFile("./1800.csv")
rdd = lines.map(parseLine)
minTemps = rdd.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect()

for result in results:
	print result[0] + "\t{:.2f}C".format(result[1])

