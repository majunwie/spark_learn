package scalaT
import scala.collection.immutable

object TFor {
  def main(args: Array[String]): Unit = {
//    val arr: List[Int] = List(1, 2, 3, 4, 5)
//    for (elem <- arr) {
//      println(elem)
//    }
//    println("----")
//    for (elem <- 1 to 10){
//      println(elem)
//    }
//    println("----")
//    for (elem <- 1 until  10){
//      println(elem)
//    }
//    println("----")
//    for (elem <- 1 until  10 if elem!=3){
//      println(elem)
//    }
    println("----")
    val a: immutable.Seq[Int] = for {
      elem <- 1 until  10 if elem!=3
    }yield elem
    for (d<- a){
      println(d)
    }
  }
}
