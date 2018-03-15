package jingli

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt
import java.io._

object Assignment2_1 {
  
  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()
    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
       movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }
  
    def parseLine(line: String) = {
      val fields = line.split("\t")
      val movieID = fields(1).toInt
      val ratings = fields(2).toDouble
      (movieID, ratings)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Assignment2_1")
    
    println("\nLoading movie names...")
    val nameDict = sc.broadcast(loadMovieNames)
    
    val data = sc.textFile("../ml-100k/u.data")

    // Map ratings to key / value pairs: movie ID, rating
    val rdd = data.map(parseLine)
    
    // Assign 1 whenever a new movieID appeared
    val avgRatings1 = rdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    
    // Only calculate the average rating for movies with at least 100 ratings
    val avgRatings2 = avgRatings1.filter(x => x._2._2 >= 100)
    val avgRatings3 = avgRatings2.map(x => (nameDict.value(x._1), x._2._1 / x._2._2, x._2._2)).collect().sortBy(_._2)
    avgRatings3.foreach(println)
    
    //
    val file = "mostPopMovies.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- avgRatings3) {
        writer.write(x + "\n")
                    }
    writer.close()
    
  }
}