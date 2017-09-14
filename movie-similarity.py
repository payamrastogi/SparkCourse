import sys

from pyspark import SparkConf, SparkContext
from math import sqrt

def loadMovieNames():
	movieNames = {}
	with open("./ml-100k/u.ITEM") as f:
		for line in f:
			fields = line.split("|")
			movieNames[int(fields[0])] = fields[1].decode("ascii", "ignore")
	return movieNames

def makePairs((user, ratings)):
	(movie1, rating1) = ratings[0]
	(movie2, rating2) = ratings[1]
	return ((movie1, movie2), (rating1, rating2))

def filterDuplicates((userId, ratings)):
	(movie1, rating1) = ratings[0]
	(movie2, rating2) = ratings[1]
	return movie1 < movie2

def computeCosineSimilarity(ratingPairs):
	numPairs = 0
	sum_xx = sum_yy = sum_xy =0
	for ratingX, ratingY in ratingPairs:
		sum_xx += ratingX * ratingX
		sum_yy += ratingY * ratingY
		sum_xy += ratingX * ratingY
		numPairs += 1

	numerator = sum_xy
	denominator = sqrt(sum_xx) * sqrt(sum_yy)

	score = 0
	if(denominator):
		score = (numerator / float(denominator))

	return (score, numPairs)


conf = SparkConf().setMaster("local[*]").setAppName("MovieSimilarities")
sc = SparkContext(conf=conf)

print "\nLoding movie names"
nameDict = loadMovieNames()

data = sc.textFile("./ml-100k/u.data")

ratings = data.map(lambda x: x.split()).map(lambda x: (int(x[0]), (int(x[1]), float(x[2]))) )


joinedRatings = ratings.join(ratings)

uniqueJoinedRatings = joinedRatings.filter(filterDuplicates)

moviePairs = uniqueJoinedRatings.map(makePairs)

moviePairRatings = moviePairs.groupByKey()

moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()

#moviePairSimilarities.sortByKey()
#moviePairSimilarities.saveAsTextFile("movie-sims")

if(len(sys.argv) > 1):
	scoreThreshold = 0.97
	coOccurenceThreshold = 50

	movieId = int(sys.argv[1])

	filteredResults = moviePairSimilarities.filter(lambda (pair, sim):\
		(pair[0]==movieId or pair[1]==movieId)\
		and sim[0] > scoreThreshold and sim[1]>coOccurenceThreshold)

	results = filteredResults.map(lambda (pair, sim): (sim, pair)).sortByKey(ascending = False).take(10)

	print "Top 10 similar movie for " + nameDict[movieId]
	for result in results:
		(sim, pair) = result

		similarityMovieId = pair[0]
		if(similarityMovieId == movieId):
			similarityMovieId = pair[1]

		print nameDict[similarityMovieId] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1])