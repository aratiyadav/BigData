import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf



object twitter{
  
  def main(args: Array[String]):Unit =
  { if(args.length <2)
    {
        System.err.println("Usage WordCount <infile> <outfile>")
        System.exit(1)
    }
  
    //val conf = new SparkConf().setAppName("twitter app").setMaster("local[2]")
    val conf = new SparkConf().setAppName("wordcount app").setMaster("yarn")
    
    val sc = new SparkContext(conf)
    
    //Reading the input file and creating RDDs
    val pair=sc.textFile(args(0)).map(line=>line.split(" "))
    
    val pairA = pair.map(splits=>((splits(0),splits(1)),1))
    val pairB = pair.map(splits=>((splits(1),splits(0)),2))

    //Join operation between pairA RDD and pairB RDD
    val join1 = pairA.join(pairB)
    
    //Join operation between pairA RDD and results of pairA 
    val join2 = pairA.leftOuterJoin(join1)
    
    
    //Filtering of keys with values as "None"
    val out = join2.filter{case(key,(value1,value2))=>(value2 == None)}
    
    //Saving the results of filteredRDD to a textfile
    val last = out.map{case((key1,key2),(value1,value2))=>key1+" "+key2}
    last.saveAsTextFile(args(1))
    
    sc.stop()
  }
  
}