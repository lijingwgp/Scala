package edu.jwilck

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Find the superhero with the most co-appearances. */
object MostPopularSuperhero {
  
  // Function to extract the hero ID and number of connections from each line
  // s+ is a regular expression to split up based on white space (tab, space, etc.)
  def countCoOccurences(line: String) = {
    var elements = line.split("\\s+")
    ( elements(0).toInt, elements.length - 1 )
  }
  
  // Function to extract hero ID -> hero name tuples (or None in case of failure)
  // Note, an Option is a Scala construct to say we do have data or don't have data
  // So we can have a None object.  Or Some object.
  // Similar to a Nil or Null value in other languages.
  def parseNames(line: String) : Option[(Int, String)] = {
    var fields = line.split('\"')
    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")   
    
    // Build up a hero ID -> name RDD
    // In earlier examples, we used a scala map object or broadcast variables
    // This way is to use an RDD (to send out to nodes)
    // flatMap is used to deal with empty lines or other issues
    val names = sc.textFile("../MarvelNames.txt")
    val namesRdd = names.flatMap(parseNames)
    
    // Load up the superhero co-apperarance data
    val lines = sc.textFile("../MarvelGraph.txt")
    
    // Convert to (heroID, number of connections) RDD
    val pairings = lines.map(countCoOccurences)
    
    // Combine entries that span more than one line
    val totalFriendsByCharacter = pairings.reduceByKey( (x,y) => x + y )
    
    // Flip it to # of connections, hero ID
    val flipped = totalFriendsByCharacter.map( x => (x._2, x._1) )
    
    // Find the max # of connections
    val mostPopular = flipped.max()
    
    // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
    val mostPopularName = namesRdd.lookup(mostPopular._2)(0)
    
    // Print out our answer!
    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.") 
  }
  
}
