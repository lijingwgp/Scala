package jingli

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQL {
  
  // Define a new class called "Person"
  // ID:Int, name:String, age:Int, numFriends:Int are the column names of the associated data set
  // and the column data structures
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)
  
  def mapper(line:String): Person = {
    // Read line by line
    // In each line, elements are separated by comma
    // Note that this mapper function helps build a structured RDD
    val fields = line.split(',')  
    // Calls the defined class object and pass in fields elements which are row observations
    // under each column from the data set
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    // Always close the new created SparkSession afterwards
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    
    // Call in data set using sparkContext 
    // Since it is not already a database, we have to create a structured data set 
    // then convert to database 
    val lines = spark.sparkContext.textFile("../friends.csv")
    // Use the mapper function to create RDD
    // Note that if it were already a database, we could have read it in directly
    // using spark.read.json("file path")
    val people = lines.map(mapper)
    
    // Infer the schema, and register the DataSet as a table.
    // Need the import spark.implicits._ when inferring the schema (i.e., when doing .toDS)
    // This step is to convert the structured RDD to a data set
    import spark.implicits._
    val schemaPeople = people.toDS
    
    // This line of code prints out each column's name, data type and whether NA's are allowed
    // Now "people" is basically an SQL database that we can call and using SQL
    schemaPeople.printSchema()
    schemaPeople.createOrReplaceTempView("people")

    // SQL can be run over DataFrames that have been registered as a table
    val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val results = teenagers.collect()
    
    results.foreach(println)
    
    // Stop the SparkSession
    spark.stop()
  }
}