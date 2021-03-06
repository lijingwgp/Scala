object LearningScala1 {
  // VALUES are immutable constants. You can't change them once defined.
  val hello: String = "Hello!"                    //> hello  : String = Hello!
  println(hello)                                  //> Hello!
  
  // Notice how Scala defines things backwards from other languages - you declare the
  // name, then the type.
  
  // VARIABLES are mutable
  var helloThere: String = hello                  //> helloThere  : String = Hello!
  helloThere = hello + " There!"
  println(helloThere)                             //> Hello! There!
 
  // One key objective of functional programming is to use immutable objects as often as possible.
  // Try to use operations that transform immutable objects into a new immutable object.
  // For example, we could have done the same thing like this:
  val immutableHelloThere = hello + " There!"     //> immutableHelloThere  : String = Hello! There!
  println(immutableHelloThere)                    //> Hello! There!
  
  // Some other types
  val numberOne : Int = 1                         //> numberOne  : Int = 1
  val truth : Boolean = true                      //> truth  : Boolean = true
  val letterA : Char = 'a'                        //> letterA  : Char = a
  val pi : Double = 3.14159265                    //> pi  : Double = 3.14159265
  val piSinglePrecision : Float = 3.14159265f     //> piSinglePrecision  : Float = 3.1415927
  val bigNumber : Long = 1234567890l              //> bigNumber  : Long = 1234567890
  val smallNumber : Byte = 127                    //> smallNumber  : Byte = 127
  
  // String printing tricks
  // Concatenating stuff with +:
  println("Here is a mess: " + numberOne + truth + letterA + pi + bigNumber + helloThere)
                                                  //> Here is a mess: 1truea3.141592651234567890Hello! There!
  
  // printf style: (println means print line)
  println(f"Pi is about $piSinglePrecision%.3f")  //> Pi is about 3.142
  println(f"Zero padding on the left: $numberOne%05d")
                                                  //> Zero padding on the left: 00001
                                                  
  // Substituting in variables:
  println(s"I can use the s prefix to use variables like $numberOne $truth $letterA")
                                                  //> I can use the s prefix to use variables like 1 true a
  // Substituting expressions (with curly brackets); even functions, etc.:
  println(s"The s prefix isn't limited to variables; I can include any expression. Like ${1+2}")
                                                  //> The s prefix isn't limited to variables; I can include any expression. Like
                                                  //|  3
                                                 
  // Using regular expressions:
  val SheldonsFavorite: String = "the number 73 is actually pretty special"
                                                  //> SheldonsFavorite  : String = the number 73 is actually pretty special
  val pattern = """.* ([\d]+).*""".r              //> pattern  : scala.util.matching.Regex = .* ([\d]+).*
  val pattern(answerString) = SheldonsFavorite    //> answerString  : String = 73
  val answer = answerString.toInt                 //> answer  : Int = 73
  println(answer)                                 //> 73
  
  // Dealing with booleans
  val isGreater = 1 > 2                           //> isGreater  : Boolean = false
  val isLesser = 1 < 2                            //> isLesser  : Boolean = true
  val impossible = isGreater & isLesser           //> impossible  : Boolean = false
  val anotherWay = isGreater && isLesser          //> anotherWay  : Boolean = false
  
  val prof: String = "Wilck"                      //> prof  : String = Wilck
  val bestprof: String = "Wilck"                  //> bestprof  : String = Wilck
  val isBest: Boolean = prof == bestprof          //> isBest  : Boolean = true
  
  // EXERCISE
  // Write some code that takes the value of pi, doubles it, and then prints it within a string with
  // three decimal places of precision to the right.
  // Just write your code below here; any time you save the file it will automatically display the results!

 
}