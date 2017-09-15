from pyspark import SparkContext, SparkConf

def loadGenres():
	genres = {}
	with open("./ml-100k/u.GENRE") as f:
		for line in f:
			fields = line.split("|")
			if (fields!=None and len(fields) > 1):
				genres[int(fields[1])] = fields[0].decode("ascii", "ignore")
	return genres

# input > 7|Twelve Monkeys (1995)|01-Jan-1995||http://us.imdb.com/M/title-exact?Twelve%20Monkeys%20(1995)|0|0|0|0|0|0|0|0|1|0|0|0|0|0|0|1|0|0|0
# output > 1543: (u'Johns (1996)', [u'Drama'])
def loadMovieData():
	genres = loadGenres()
	movieData = {}
	with open("./ml-100k/u.ITEM") as f:
		for line in f:
			fields = line.split("|")
			if(len(fields)== 24):
				genre = []
				for i in range(5, 24):
					if(int(fields[i])==1):
						genre.append(genres[i-5])
				movieData[int(fields[0])] = (fields[1].decode("ascii", "ignore"), genre)
	return movieData

movieData = loadMovieData()
#genres = loadGenres()
#print movieData

# input > 196	242	3	881250949
#output > (13, (225, 2.0))...
def userMovieRating(line):
	fields = line.split()
	userId = int(fields[0])
	movieId = int(fields[1])
	rating = float(fields[2])
	return (userId, (movieId, rating))

def filterOutBadRatings((userId, (movieId, rating))):
	return rating > 2

def filterDuplicates((userId, ratings)):
	(movie1, rating1) = ratings[0]
	(movie2, rating2) = ratings[1]
	return movie1 < movie2

def makePairs((user, ratings)):
	(movie1, rating1) = ratings[0]
	(movie2, rating2) = ratings[1]
	return ((movie1, movie2), (rating1, rating2))


conf = SparkConf().setMaster("local[*]").setAppName("Movies Recommendation Better")
sc = SparkContext(conf = conf)

data = sc.textFile("./ml-100k/u.data")
userMovieRatings = data.map(userMovieRating)
filteredUserMovieRatings = userMovieRatings.filter(filterOutBadRatings)

#results = userMovieRatings.collect()
#print results

joinedUserMovieRatings = filteredUserMovieRatings.join(filteredUserMovieRatings)
#output > (511, ((340, 4.0), (1527, 4.0))), (511, ((340, 4.0), (343, 3.0))), (511, ((340, 4.0), (678, 2.0)))
uniqueUserMovieRatings = joinedUserMovieRatings.filter(filterDuplicates)
#output > ((340, 682), (4.0, 4.0)), ((340, 887), (4.0, 5.0)), ((340, 1527), (4.0, 4.0)), ((340, 343), (4.0, 3.0)), ((340, 678), (4.0, 2.0))
uniqueUserMovieRatingPairs = uniqueUserMovieRatings.map(makePairs)
results = uniqueUserMovieRatingPairs.collect()
print results