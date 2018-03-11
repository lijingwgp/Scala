package jingli

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt
import java.io._

object TeamAssignment2_2 {
  
  def parseLine(line:String) = {
    val fields = line.split(",")
    val customerID = fields(0).toInt
    val spent_amount = fields(2).toDouble
    (customerID, spent_amount)
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","TeamAssignment2_2")
    val lines = sc.textFile("../DataA1.csv")

    val rdd = lines.map(parseLine)
    val total_spent = rdd.reduceByKey((x, y) => (x + y))
    
    val flipped = total_spent.map(x => (x._2, x._1))
    val flipped_sort = flipped.sortByKey()
    val flipped_back = flipped_sort.map(x => (x._2, x._1))
    
    val results2 = flipped_back.collect()
    results2.sorted.foreach(println)
    
    val file2 = "teamAssignAnswer2.txt"
    val writer2 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file2)))
    for (x <- results2) {
        writer2.write(x + "\n")
                    }
    writer2.close()
  }
  
}