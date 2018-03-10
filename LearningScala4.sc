object LearningScala4 {
  // Data structures
  
  // Tuples (Also really common with Spark!!)
  // Immutable lists
  // Often thought of as database fields, or columns.
  // Useful for passing around entire rows of data.
  
  val Stuff = ("William", "&", "Mary")            //> Stuff  : (String, String, String) = (William,&,Mary)
  println(Stuff)                                  //> (William,&,Mary)
  
  // You refer to individual fields with their ONE-BASED index (i.e., it starts counting at 1):
  println(Stuff._1)                               //> William
  println(Stuff._2)                               //> &
  println(Stuff._3)                               //> Mary
 
 // You can create a key/value pair with ->
 val MoreStuff = "Big Data" -> "BUAD 5722"        //> MoreStuff  : (String, String) = (Big Data,BUAD 5722)
 println(MoreStuff._2)                            //> BUAD 5722
 
 // You can mix different types in a tuple
 val aBunchOfStuff = ("Wilck", 5722, true)        //> aBunchOfStuff  : (String, Int, Boolean) = (Wilck,5722,true)
 
 // Lists
 // Like a tuple, but it's an actual Collection object that has more functionality.
 // Also, it cannot hold items of different types.
 // It's a singly-linked list under the hood.
 
 val profList = List("Bradley", "Murray", "Blossom", "Wilck")
                                                  //> profList  : List[String] = List(Bradley, Murray, Blossom, Wilck)
 
 // Access individual members using () with ZERO-BASED index (confused yet?) (i.e., it starts counting at 0)
 println(profList(1))                             //> Murray
 
 // head and tail give you the first item, and the remaining ones.
 println(profList.head)                           //> Bradley
 println(profList.tail)                           //> List(Murray, Blossom, Wilck)
 
 
 // Iterating though a list
 for (prof <- profList) {println(prof)}           //> Bradley
                                                  //| Murray
                                                  //| Blossom
                                                  //| Wilck
 
 // Let's apply a function literal to a list, map() can be used to apply any function to every item in a collection.
val backwardProfs = profList.map( (prof: String) => {prof.reverse})
                                                  //> backwardProfs  : List[String] = List(yeldarB, yarruM, mossolB, kcliW)
 for (prof <- backwardProfs) {println(prof)}      //> yeldarB
                                                  //| yarruM
                                                  //| mossolB
                                                  //| kcliW
                                                  
// reduce() can be used to combine together all the items in a collection using some function.
val numberList = List(1, 2, 3, 4, 5)              //> numberList  : List[Int] = List(1, 2, 3, 4, 5)
val sum = numberList.reduce( (x: Int, y: Int) => x + y)
                                                  //> sum  : Int = 15
println(sum)                                      //> 15

// filter() can remove stuff you don't want. Here we'll introduce wildcard syntax while we're at it.
val iHateFives = numberList.filter( (x: Int) => x != 5)
                                                  //> iHateFives  : List[Int] = List(1, 2, 3, 4)
val iHateThrees = numberList.filter(_ != 3)       //> iHateThrees  : List[Int] = List(1, 2, 4, 5)

// Note that Spark has its own map, reduce, and filter functions that can distribute these operations. But they work the same way.

// Concatenating lists (using ++ operator)
val moreNumbers = List(6, 7, 8)                   //> moreNumbers  : List[Int] = List(6, 7, 8)
val lotsOfNumbers = numberList ++ moreNumbers     //> lotsOfNumbers  : List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8)

// More list fun
val reversed = numberList.reverse                 //> reversed  : List[Int] = List(5, 4, 3, 2, 1)
val sorted = reversed.sorted                      //> sorted  : List[Int] = List(1, 2, 3, 4, 5)
val lotsOfDuplicates = numberList ++ numberList   //> lotsOfDuplicates  : List[Int] = List(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
val distinctValues = lotsOfDuplicates.distinct    //> distinctValues  : List[Int] = List(1, 2, 3, 4, 5)
val maxValue = numberList.max                     //> maxValue  : Int = 5
val total = numberList.sum                        //> total  : Int = 15
val hasThree = iHateThrees.contains(3)            //> hasThree  : Boolean = false

// Maps
// Useful for key/value lookups on distinct keys
// Like dictionaries in other languages

val classMap = Map("Bradley" -> "CTBA", "Murray" -> "Machine Learning", "Wilck" -> "Big Data", "Blossom" -> "AI")
                                                  //> classMap  : scala.collection.immutable.Map[String,String] = Map(Bradley -> 
                                                  //| CTBA, Murray -> Machine Learning, Wilck -> Big Data, Blossom -> AI)
println(classMap("Murray"))                       //> Machine Learning

// Dealing with missing keys
println(classMap.contains("Koehl"))               //> false

val koehlclass = util.Try(classMap("Koehl")) getOrElse "Unknown"
                                                  //> koehlclass  : String = Unknown
println(koehlclass)                               //> Unknown

// EXERCISE
// Create a list of the numbers 1-20; your job is to print out numbers that are evenly divisible by three. (Scala's
// modula operator, like other languages, is %, which gives you the remainder after division. For example, 9 % 3 = 0
// because 9 is evenly divisible by 3.) Do this first by iterating through all the items in the list and testing each
// one as you go. Then, do it again by using a filter function on the list instead.

}