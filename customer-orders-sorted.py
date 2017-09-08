from pyspark import SparkConf, SparkContext

def customer(line):
	fields = line.split(",")
	customerId = int(fields[0])
	orderNo = int(fields[1])
	amount = float(fields[2])
	return (customerId, amount)

conf = SparkConf().setMaster("local").setAppName("Customers")
sc = SparkContext(conf = conf)

lines = sc.textFile("./customer-orders.csv")
rdd = lines.map(customer)
customerAmount = rdd.reduceByKey(lambda x, y: x+y)
customerAmountSorted = customerAmount.map(lambda (x, y):(y,x)).sortByKey()
results = customerAmountSorted.collect()

for result in results:
	print str(result[1]) + "\t{:.2f}".format(result[0])