package jingli

/** Load packages */
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Compute the average number of friends by age in a social network. */
object FriendsByAge {
  
  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) = {
      // Split by commas
      val fields = line.split(",")
      // Extract the age and numFriends fields, and convert them to integers
      // Age is Key, and number of friends is Value
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      // Create a tuple and return this tuple as our result
      // This is to map pairs of data into the RDD using tuples
      // This is our key-value pair
      // (33,285), (26,2), (55,221), (40,465)
      (age, numFriends)
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "FriendsByAge")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile("../friends.csv")
    
    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)
    
    // We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    
    // We use mapValues to transform each numFriends value to a tuple of (numFriends, 1)
    // Note that x indicates each numFriends
    // (33,385) => (33,(385,1))
    // (33,2) => (33,(2,1))
    // (55,221) => (55,(221,1))
    // (55,5) => (55,(5,1))
    
    // Then we use reduceByKey to sum up the total numFriends and total occurrences for each age, 
    // by adding together all the numFriends values and 1's respectively
    // (33,(387,2))
    // (55,(226,2))
    
    // reduceByKey() combines values with the same key using some function
    // For example, if key=20; then the function below says given two values for key=20, (385,1) and (2,1)
    // add those two values together
    
    // Note that x and y represent two values (not a key-value pair)
    // For example, if there are two (x,y) which both are values for key=20, and they are (385,1) and (2,1) respectively
    // Then x1=385, x2=1, and y1=2, y2=1
    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    
    // So now we have tuples of (age, (totalFriends, totalInstances))
    // To compute the average we divide totalFriends / totalInstances for each age
    // For example, (33,(378,2)) => (33,193.5)
    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)
    
    // Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    // This step calls an action
    val results = averagesByAge.collect()
    
    // Sort and print the final results
    results.sorted.foreach(println)
  }
}
  