package jingli

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt
import java.io._

object TeamAssignment2_1 {
  
  def parseLine(line:String) = {
    val fields = line.split(",")
    val customerID = fields(0).toInt
    val spent_amount = fields(2).toDouble
    (customerID, spent_amount)
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","TeamAssignment2_1")
    val lines = sc.textFile("../DataA1.csv")

    val rdd = lines.map(parseLine)
    val total_spent = rdd.reduceByKey((x, y) => (x + y))
    
    val results1 = total_spent.collect()
    val sortedResults = results1.toSeq.sortBy(_._1)
    
    val file1 = "teamAssignAnswer1.txt"
    val writer1 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file1)))
    for (x <- sortedResults) {
        writer1.write(x + "\n")
                    }
    writer1.close()
  }
}
