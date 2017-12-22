import org.apache.spark.mllib.recommendation._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import spark.implicits._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

import org.apache.spark.SparkConf
object recommender {
  def main(args: Array[String]): Unit = {
    if (args.length < 2){
        System.err.println("Usage: symmetric app <input file>  <output file>")
        System.exit(1);  }

//Data Cleaning

//artist_alias.txt
val artistRDD = sc.textFile("hdfs://armaster:9000/hadoop-user/data/artist_alias.txt").map(line=>line.split("\\W+")).filter(splits=>splits(0) != "" && splits(1) != "").map(splits=>((splits(0).toInt), splits(1).toInt))

//UserArtist.txt
val userArtistRDD = sc.textFile("hdfs://armaster:9000/hadoop-user/data/user_artist_data.txt").map(line=>line.split("\\W+")).map(splits=>((splits(1).toInt), (splits(0).toInt, splits(2).toInt)))

//OuterJoin
val joined = userArtistRDD.leftOuterJoin(artistRDD).map{case((artistId), ((userId, countOfPlayed), artistId2))=>(userId.toInt, artistId.toInt, artistId2.getOrElse(0).toInt, countOfPlayed.toInt)}


//Replacing artist ids with valid artist ids
val joinFilterOnArtistId = joined.map{case(userId, artistId, artistId2, countOfPlayed) => (userId,countOfPlayed,
if(artistId2 == 0) artistId.toInt
 else artistId2)}

val finalRDD = joinFilterOnArtistId.map{case(userId, countOfPlayed, artistId)=>Rating(userId.toInt, artistId.toInt, countOfPlayed.toInt)}


//Applying ALS transformation

//Divinding teh dataset into training and testing
val Array(training, testing) = finalRDD.randomSplit(Array(0.8, 0.2))

//Setting parameters
val rank=10; val lambda=0.01 ;val numIterations= 20; val alpha = 0.01

//Building the model
val model = ALS.trainImplicit(training, rank, numIterations, lambda, alpha)

//Create Checkpoint
sc.setCheckpointDir("hdfs://Armaster:9000/RddCheckPoint")
artistRDD.checkpoint
userArtistRDD.checkpoint

//Evaluating ALS model

val userproducts=testing.map{case Rating(user,product,rate)=>(user,product)}
val actual = testing.map{case Rating(user,product,rate)=>(user, (product,rate))}.groupByKey
val predictions=model.predict(userproducts).map{case Rating(user,product,rate)=>(user, (product,rate))}.groupByKey

/**Logic for applying rank fuction
val joined = actual.join(predictions).map{(user, (Iterable[(product, rating)], Iterable[(product1, recommended_rating)]))=>(user, rank(actual:Array[(product,rate)],predicted:Array[(product1,recommended_rating))}
**/


//RANK FUNCTION 
//Actual
val actualRank = testing.map{case Rating(user,product,rate)=>((product),rate)}


//Predictions
val predictionsRank=model.predict(userproducts).map{case Rating(user,product,rate)=>((product),rate)}
	
//Sorting the predictions on the rating value in descending order
val predictions_sorted = predictionsRank.sortBy(x =>-x._2.toDouble).map{case((product),rating)=>((product),rating)}

//Calculating Percentile rank 
val predictionCount = predictions_sorted.count
val finalPredictions = predictions_sorted.zipWithIndex.map{case((product, rating), indexOfRank)=>((product), indexOfRank/predictionCount)}


//Sorting by ArtistId
val actual_sorted = actualRank.sortByKey()
val predicted_sorted = finalPredictions.sortByKey()

//Have not implemented the below code hence presenting only the logic

//Logic for calculating the rank
/**
rank(actual:Array[(artistId,rate)],predicted:Array[(artistId,percentileRank)):Double={
val actual_set= actual.toSet
val predicted_set= predicted.toSet
val sum=0
val product=0
val finalproduct=0
for(i<-1 to actual_set.size)
{
	for(j<-1 to predicted_set)
	{
		product= i.rate*j.percentileRank

	}
	sum+=i.rating
	productFinal+= product
	return rank=productFinal/sum
}
}
**/










