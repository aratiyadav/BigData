import org.apache.spark.mllib.recommendation._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import java.io._;


import org.apache.spark.SparkConf
object recommender_assign {
  def main(args: Array[String]): Unit = {
    if (args.length < 2){
        System.err.println("Usage: symmetric app <input file1> <input file2>  <output file>")
        System.exit(1);  }
       //Creating a spark context
    val conf = new SparkConf().setAppName("recommender app <input file> <output file>").setMaster("yarn")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("/hadoop-user/spark/checkpointing")
    

//Getting the rank function given an array of actual rating and another array of recommended ratings
def evaluateImplicit(actual:Array[Rating], recommended:Array[Rating]):Double={
var numerator=0.0; var denominator=0.0; var rank=1.0;
val recommended_sorted=recommended.sortBy(r=> r.rating).reverse.map(r=>r.product)

for(r<-actual){
     val index = recommended_sorted.indexOf(r.product)
      
     rank= index.toDouble/recommended.length
     denominator= denominator +r.rating
     numerator=numerator+rank*r.rating
    
}
      
numerator/denominator

}
    
//creating an rdd(artist_id, (user_id,rating)) from the user_artist_data file
val ratings = sc.textFile(args(0)).map(line=>line.split("\\s+")).map(splits=>(splits(1).toInt,(splits(0).toInt, splits(2).toDouble)))

//creating an rdd(bad_artist_id,good_artist_id) from the artist_alias file
val artist_alias = sc.textFile(args(1)).map(line=>line.split("\\s+")).filter(line=>line.length>1 && line(0)!="" && line(1)!="").map(splits=>(splits(0).toInt,splits(1).toInt))

//replacing the bad_ids with good ids in ratingsrdd using leftouterjoin and map.The result is store in ratings_cleaned rdd which 
//is in form of (Rating(user, good_artist_id, rating))
val ratings_cleaned= ratings.leftOuterJoin(artist_alias).map{case(itemId, ((userid,rating),goodId))=>Rating(userid, goodId.getOrElse(itemId), rating)}

//Randomly splitting the data into training and testing set
val Array(training,testing)=ratings_cleaned.randomSplit(Array(0.8,0.2))

//Training the als implicit model
val model =ALS.trainImplicit(training,20,20)

//building an rdd of the form (user_id,artist_id) from the testing set
val userProducts= testing.map(r=>(r.user,r.product))

//Using the model to predict ratings for the elements in userProducts rdd
val recommendations=model.predict(userProducts).map(r=>(r.user, r)).groupByKey()

//group the testing and predicted set by user and join them. The result is stored in joined which is an rdd of the form (user, actual:Array[Rating], predicted:Array[Rating])
val joined = testing.map(r=>(r.user, r)).groupByKey().join(recommendations).map{case(u,(actual, rec))=>(actual.toArray, rec.toArray)}
 
//call the evaluateImplicit for each user/element in joined to compute the rank and take the average of the ranks over users.
val avgrank=joined.map{case(actual,recommended)=>evaluateImplicit(actual,recommended)}.mean
  
println(avgrank)
sc.stop()
  }
}
