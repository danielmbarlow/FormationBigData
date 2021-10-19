import scala.collection.immutable._

object ScalaTraining {
  def main(args: Array[String]) : Unit = {
    val myVariable : String = "hello world!"

    println(myVariable)

    extractFirst(myVariable, 3)

    println(extractFirstFunction(myVariable, 4))

    println("== for loop")
    for( i <- 1 to 10) {
      println(s"here's the value of ${i}")
    }

    val myList : List[String] = List("Daniel", "Juvenal", "Taylor", "Adrien", "Bruno")

    println("== Extract by index")
    println(myList(0))

    println(s"== #foreach")
    myList.foreach(name => println(name))

    println(s"== #map")
    val mappedList = myList.map ( name => s"${name} mapped" )
    println(mappedList)

    println(s"== Filter")
    println(myList.filter(name => name == "Daniel"))

    println(s"== Count")
    println(myList.count(name => name == name))

    println(s"== Placeholder syntax")
    val intList: List[Int] = List(1,2,3,4,5,6)
    println(intList.map(_*2))

    println("== Tuples")
    val myTp : (String, Int, Boolean, String) = ("Daniel", 42, true, "Learning")
    println(myTp)

    println(s"== Maps")
    val map1 : Map[String, String] = Map(
      "IP" -> "192.168.1.1",
      "Host" -> "ServerName",
      "port" -> "3392"
    )
    println(map1)
    println(map1.keys)
    println(map1("IP"))
    println(map1.values)

    val map3 : Map[String, List[String]] = Map(
      "cities" -> List("Paris", "Tokyo", "New York"),
      "countries" -> List("France", "Japan", "US")
    )

    println("== All values")
    println(map3.values.flatMap(f => f))

    println("== Arrays")
    val a: Array[String] = Array("a", "b", "c", "d")
    a.foreach(entry => println(s"Entry ${entry}"))
  }

  def extractFirst(text: String, start: Int): Unit = {
    val newVar: String = text.substring(start)
    println(newVar)
  }

  def extractFirstFunction(text: String, start: Int): String = {
    return text.substring(start)
  }
}
