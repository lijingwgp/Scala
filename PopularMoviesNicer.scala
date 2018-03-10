package jingli

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import java.io._

/** Find the movies with the most ratings. */
object PopularMoviesNicer {
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item
    // Ints indicate movieID, String indicate movieNames
    var movieNames:Map[Int, String] = Map()
    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    // For all elements from each lines
    for (line <- lines) {
      // Get fields that are seperated by '|'
      var fields = line.split('|')
      if (fields.length > 1) {
       // Map the first element which is movieID to the second element which is movieNames
       // So variable movieNames contains both integers and strings
       movieNames += (fields(0).toInt -> fields(1))
      }
    }    
    return movieNames
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMoviesNicer") with Serializable
    
    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames)
    
    // Read in each rating line
    val lines = sc.textFile("../ml-100k/u.data")
    
    // Map to (movieID, 1) tuples
    // Convert movieID to Int
    // x's are the (key, value) pairs
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    
    // Count up all the 1's for each movie
    // x is the previous running total
    // y is the next occuring element
    val movieCounts = movies.reduceByKey((x, y) => x + y )
    
    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map( x => (x._2, x._1) )
    
    // Sort
    val sortedMovies = flipped.sortByKey()
    
    // Fold in the movie names from the broadcast variable
    // Recall that x._2 now represent the movieID after flip happened
    val sortedMoviesWithNames = sortedMovies.map(x => (nameDict.value(x._2), x._1))
    
    // Collect and print results
    val results = sortedMoviesWithNames.collect()
    
    results.foreach(println)
    
    val file = "PopMoviesNicer.txt"    
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- results) {
        writer.write(x + "\n")
    }
    writer.close()    
  }  
}

