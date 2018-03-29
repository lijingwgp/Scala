package jingli

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.sql.functions._

    
object Assignment3_1 {
  
  /** Load up a Map of movie IDs to movie names. */
  // Map[Int, String] indicates passing in an integer and returning a string
  def loadMovieNames(): Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a mapping variable that connects movieIDs which are integers to movieNames which are strings
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
       movieNames += (fields(0).toInt -> fields(1))
       }
    }
     return movieNames
  }
  
  // Define a case class called movie
  case class Movie(movieID: Int, rating: Int)
  
  def mapper(line:String): Movie = {
    // Read line by line
    // In each line, elements are separated by tab
    // Note that this mapper function helps build a structured RDD
    val fields = line.split('\t')  
    // Calls the defined class object and pass in fields elements which are row observations
    // under each column from the data set
    val movie:Movie = Movie(fields(1).toInt, fields(2).toInt)
    return movie
  }
  
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("Assignment3_1")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows
      .getOrCreate()
    
    // Read in each rating line and extract the movie ID; construct an RDD of Movie objects
    // Note that each time we use map() we want to call the class name
    val lines = spark.sparkContext.textFile("../ml-100k/u.data")
    
    // Using the mapper to create a RDD
    val movies = lines.map(mapper)
    
    // Creating data frame
    import spark.implicits._
    val schemaMovie = movies.toDS
    schemaMovie.printSchema()
    schemaMovie.createOrReplaceTempView("movie")
    
    // SQL syntax
    val topMovies = spark.sql("SELECT movieID, AVG(rating), COUNT(movieID) FROM movie GROUP BY (movieID) HAVING COUNT(movieID) >= 100 ")

    val results = topMovies.collect()
    val names = loadMovieNames()
    println
    for (result <- results) {
      // result is just a Row at this point; we need to cast it back.
      // Each row has movieID, count as above.
      println (names(result(0).asInstanceOf[Int]) + ": " + result(1) + "," + result(2))
    }
    spark.stop()
  }
}