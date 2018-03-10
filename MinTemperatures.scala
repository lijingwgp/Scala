package jingli

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.min

/** Find the minimum temperature by weather station */
object MinTemperatures {
  
  def parseLine(line:String)= {
    // Read data line by line
    val fields = line.split(",")
    // Key
    val stationID = fields(0)
    // Value is selected based on entryType
    val entryType = fields(2)
    // Convert temperature; temperature is the value
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }
  
    /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MinTemperatures")
    
    // Read each line of input data
    val lines = sc.textFile("../1800weather.csv")
    
    // Convert to (stationID, entryType, temperature) tuples
    val parsedLines = lines.map(parseLine)
    
    // Filter out all but TMIN entries
    // Here we use x._2 because that is to refer to the second element of the 
    // (stationID, entryType, temperature) tuples
    val minTemps = parsedLines.filter(x => x._2 == "TMIN")
    
    // Convert to (stationID, temperature)
    // This is the key/value pair that we want after applying the filter
    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
    
    // Reduce by stationID retaining the minimum temperature found
    // x,y are both values for the keys, x represents the running totals
    // y represents 1 whenever the same movieID appeared
    // So min(x,y) represents compare y to all of the previous x's
    val minTempsByStation = stationTemps.reduceByKey((x,y) => min(x,y))
    
    // Collect, format, and print the results
    val results = minTempsByStation.collect()
    
    for (result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedTemp = f"$temp%.2f F"
       println(s"$station minimum temperature: $formattedTemp") 
    }
  }
}