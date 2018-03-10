package edu.jwilck

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import java.io._

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCountBetterSorted {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using the local machine
    val sc = new SparkContext("local[*]", "WordCountBetterSorted") //with Serializable
         
    // Load each line of my book into an RDD
    val input = sc.textFile("../pg100.txt")
    
    // Split using a regular expression that extracts words
    val words = input.flatMap(x => x.split("\\W+"))
    
    // Normalize everything to lowercase
    val lowercaseWords = words.map(x => x.toLowerCase())
    
    // Count of the occurrences of each word
    // 
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
           
    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    // The first time when we flip the order: word => x._1, count => x._2
    // The second time when we flip the order: count => x._1, word => x._2
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()
    val flip = wordCountsSorted.map( x => (x._2, x._1))
    
    val results = flip.collect()
    val file = "WordCountOutputBetterSorted.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- results) {
        writer.write(x + "\n")
                    }
    writer.close()    
  }
}

