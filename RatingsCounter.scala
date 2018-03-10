package jingli

/** Import packages */
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {
 
  /** Our main function where the work is occurring */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext 
    // * indicates using every core of the local machine
    // The context is named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")
    
    // Calling the SparkContext to import the data
    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../ml-100k/u.data")
    
    // The original file format is userID, movieID, rating, timestamp
    // Convert each line to a string, split it up by tabs, and extract the third field (movie ratings)
    // Note that we only need the third field, so throwing out the data we don't need
    // Note that list index starts at 0
    // This is equivalent to the mapping step in MapReduce
    val ratings = lines.map(x => x.toString().split("\t")(2))
    
    // Count up how many times each value (rating) occurs
    // countByValue() is an action, so Spark does the work as soon as it sees an action
    // This action maps identifiers (Ratings) to how many times they occurred
    // This is equivalent to the reduce step in MapReduce
    val results = ratings.countByValue()
    
    // There are some other built-in functions as well
    // groupByKey(): group values with the same key
    // sortByKey(): sort by key values
    // keys(), values(): create an RDD with just keys, or just the values
    
    // The reading lines and mapping steps are stage 1, and the reducing step is stage 2
    // Spark creates an Execution Plan which is to separate each stages
    // Stages are then separated into tasks
    
    // Convert the results to a sequence, then sort
    val sortedResults = results.toSeq.sortBy(_._1)
    
    // Print each result on its own line
    sortedResults.foreach(println)
  }
}
