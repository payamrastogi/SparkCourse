from pyspark import SparkContext, SparkConf


def loadMovieNames():
	movieNames = {}
	with open("./ml-100k/u.ITEM") as f:
		for line in f:
			fields = line.split("|")
			movieNames[int(fields[0])] = fields[1]
	return movieNames

conf = SparkConf().setMaster("local").setAppName("Popular Movie Nicer")
sc = SparkContext(conf = conf)

nameDict  = sc.broadcast(loadMovieNames())

lines= sc.textFile("./ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x+y)
flipped = movieCounts.map(lambda (x, y): (y, x))
sortedMovies = flipped.sortByKey()

sortedMovieNames = sortedMovies.map(lambda (count, movie) : (nameDict.value[movie], count))

results= sortedMovieNames.collect()

for result in results:
    print(result)
