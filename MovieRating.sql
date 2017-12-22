import org.apache.spark.sql.expressions.Window

case class Movie(movieId:Int, genre:String)
case class Rating(movieId:Int, userId:Int, rating:Double)


--Reading values from Movies.dat file and converting it into a dataframe "movieDF"

val movieDF = sc.textFile("hdfs://armaster:9000/hadoop-user/data/movies.dat").map(line=>line.split("#")).map(splits=>(splits(0).toInt, splits(2))).flatMapValues(values=>values.split('|')).map{case(movieId, genre)=>Movie(movieId.toInt, genre)}.toDF


movieDF.createOrReplaceTempView("Movie")
--Reading values from Ratings.dat file and converting it into a dataframe "ratingDF"

val ratingDF = sc.textFile("hdfs://armaster:9000/hadoop-user/data/ratings.dat").map(line=>line.split("#")).map(splits=>(splits(1).toInt, splits(0).toInt, splits(2).toDouble)).map{case(movieId, userId, rating)=>Rating(movieId.toInt, userId.toInt, rating.toDouble)}.toDF

ratingDF.createOrReplaceTempView("Rating")


--Taking a Join of movie dataframe and artings dataframe to get the fields MovieId, UserId, Rating, Genre

sql("SELECT Rating.userId, Movie.genre, avg(Rating.rating) as ratings FROM Rating LEFT OUTER JOIN Movie ON Rating.movieId=Movie.movieId  group by Rating.userId, Movie.genre order by Rating.userId asc").createOrReplaceTempView("joinTable")

--Getting the rank of first five genre for every userid
sql("select userId, genre, ratings, rank() over (partition by userId order by userId, ratings DESC) as R from joinTable ").createOrReplaceTempView("rankedGenre")

sql("select userId, genre, ratings from rankedGenre where R<=5 order by userId asc, ratings desc").show(30)


