# A Really Quick Spark Demo

Shows joining a C* table to a CSV file. What could be more fun than that? Lots of things, actually, but stop complaining. This is still kind of sick if you think about it.

## Initializing the C* Data

Do these things in `cqlsh` and live:

1. `SOURCE 'schema.cql'`
2. `COPY movies from 'movies.csv'`
2. `COPY movie-genres from 'movie-genres.csv'`

### In Spark Shell

Do actual Spark things now:

```scala

// Read the movies as a Pair RDD keyed by movie_id
val movies = sc.cassandraTable("killr_video","movies").as( (i:String,y:Int,t:String) => (java.util.UUID.fromString(i),(t,y)) )

// Get the CSV into an RDD as text
val ratings = sc.textFile("file:///Users/tlberglund/workshops/spark/movie-ratings.csv")

// Parse the CSV, making a Pair RDD keyed by movie ID
val ratingsByID =
ratings.map{ line =>
             val fields = line.split(',')
             (java.util.UUID.fromString(fields(0)),fields(1).toFloat) }

val movies = sc.cassandraTable("killr_video","movies").as( (i:String,y:Int,t:String) => (java.util.UUID.fromString(i),t))

val ratingCount = ratingsByID.mapValues(v => 1).reduceByKey(_+_)
val ratingSums = ratingsByID.reduceByKey(_ + _)
val averageRatings = ratingCount.join(ratingSums).mapValues(r => r._2 / r._1)

val ratedMovies = movies.join(averageRatings)

ratedMovies.collect
```
