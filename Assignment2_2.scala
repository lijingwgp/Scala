package jingli

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt
import java.io._

object Assignment2_2 {
  
   def Friends_Sort(friends: List[(Int, Int)]) : List[Int]   = {

      friends.sortBy(allPair => (-allPair._2, allPair._1)).map(allPair => allPair._1)

    }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "Assignment2_2") with Serializable
    
    // Read data
    val data = sc.textFile("../input-f.txt")
    
    val id_tup = data.map(l => try{
      (l.split("\t")(0) + "," + l.split("\t")(1)) // one large list of IDs per line
      } catch {
        case _: java.lang.ArrayIndexOutOfBoundsException => (l.split("\t")(0) + "," + "0")
        })
    
    id_tup.foreach(println)

    /*
    // Lines are separated by tab
    // We want to make sure that all users have at least one friendsID
    val userPairs1 = data.map(x => x.split("\t")).filter(x => (x.size == 2))
    
    // Separate friendsID's that are contained in the friends list for each userID
    val userPairs2 = userPairs1.map(x => (x(0),x(1).split(",")))
    
    // Making sure all data type are integers
    // Note that the second element of friendsPairs2 is a list of many friendsID's 
    // We need to unpack this list for all userID's
    // To do that, we use flat map and pair every userID to each of the unpacked friendsID from the list
    val userPairs3 = userPairs2.flatMap(x => x._2.flatMap(y => Array((x._1.toInt,y.toInt))))
    
    // This is an intermediate step
    // Now that we have (userID1, friendsID1) ... (userID1, friendsID100) ...
    // We can build a list using self join. What is does is that using self join will return a list of 
    // (userID1, (friendsID1, friendsID2)), (userID1, (friendsID1, friendsID3)) ... 
    // (userID1, (friends1, friendsID100)) ... (userID1, (friendsID100, friendsID100))
    val friendPairs1 = userPairs3.join(userPairs3)
    
    // Now our job is to construct a list of (friendsID, friendsID) pair
    // Note that the second element from the selfjoin list is a list of tuples that contain pairs of friendsID's
    // We would want to abandon the userID and extract the friendsID pairs from the self joined list
    // (friendsID1, friendsID2), ..., (friendsID1, friendsID100), ..., (friendsID100, friendsID100)
    // Now, here is the thing...
    // (userID, (friendsID100, friendsID100)) has redundancy in its second element
    val friendPairs2 = friendPairs1.map(x => x._2).filter(x => x._1 != x._2)
    
    // Now the list we are about to generate is very interesting...
    // We are about to build a list of pairs of (friendsID, friendsID), again
    // And these pairs of friendsID's represent that there is one mutual friend between the friendsID A and friendsID B
    // The way we achieve this result is by subtracting one list from another
    
    // Initially, we have (userID1, (friendsID1, ..., friendsID100))
    // Then, we unpack the second element and get (userID1, friendsID1), ..., (userID1, friendsID100)
    // By using self join, we have (userID1, (friendsID1, friendsID2)), ..., (userID1, (friendsID1, friendsID100))
    // By using mapping, we build a list of pairs of friendsIDs for each userID
    // The only difference between the first userID/friendsID pairs and the second friendsID/friendsID pairs is that 
    // the second list of pairs is larger. Meaning that the person who's in the userID spot from the first list
    // not only represent a user, but also represent a friend of someone else's friend
    // In other words, a person could both be a user and a friend of someone else
    val friendPairs3 = friendPairs2.subtract(userPairs3)
    
    // Now we have the hard part taken care of, we will add 1 to those (friendsID, friendsID) pairs
    // representing that they have 1 mutual friend whenever such pair appears
    val friendPairs4 = friendPairs3.map(x => (x, 1))
    
    // This is equivalent to the first reducing job which we sum all 1's according to the keys
    val friendPairs5 = friendPairs4.reduceByKey((x, y) => x + y)
    
    // This is what a second mapper would do
    // Grouping the second friendsID with the total number of mutual friends
    val friendPairs6 = friendPairs5.map(x => (x._1._1, (x._1._2, x._2))).groupByKey().collect()
    
    // This is the second reducing job
    // val recommendations = friendPairs6.map(x => (x._1, sortFriends(x._2.toList))).map(tup2 => tup2._1.toString + "\t" + tup2._2.map(x=>x.toString).toArray.mkString(","))
    

    
    
    // Print results
    val file = "recommendation.txt"
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file)))
    for (x <- friendPairs6) {
        writer.write(x + "\n")
                    }
    writer.close()
    * 
    */
  }
}