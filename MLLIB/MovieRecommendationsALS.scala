package jingli

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import org.apache.spark.mllib.recommendation._

object MovieRecommendationsALS {
  
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
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MovieRecommendationsALS")
    
    println("Loading movie names...")
    val nameDict = loadMovieNames()
    
    // Read data line by line
    val data = sc.textFile("../ml-100k/u.data")
    
    // Create a Map that split the data from each line into two parts. Each part is separated by tabs
    // The first item is a userID, the second item is a movieID, and the third item is the rating for that movie
    // Note that the Rating() is a special data type to MLLIB. This data type is required for ALS
    val ratings = data.map( x => x.split('\t') ).map( x => Rating(x(0).toInt, x(1).toInt, x(2).toDouble) ).cache()
    
    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")
    
    // Parameters
    val rank = 8
    val numIterations = 20
    
    // Train the model with 20 iterations
    val model = ALS.train(ratings, rank, numIterations)
    
    // Specifying that we want to make recommendations for userID = 0
    // args() is a tab section in run configuration where we can specify userID
    val userID = args(0).toInt
    
    println("\nRatings for user ID " + userID + ":")
    
    // Limiting our RDD so that we only want to look at userID = 0
    val userRatings = ratings.filter(x => x.user == userID)
    val myRatings = userRatings.collect()
    
    for (rating <- myRatings) {
      println(nameDict(rating.product.toInt) + ": " + rating.rating.toString)
    }
    
    println("\nTop 10 recommendations:")
    
    // Making 10 recommendations for the specified userID
    val recommendations = model.recommendProducts(userID, 10)
    for (recommendation <- recommendations) {
      println( nameDict(recommendation.product.toInt) + " score " + recommendation.rating )
    }
  }
}