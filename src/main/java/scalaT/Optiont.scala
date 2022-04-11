package scalaT

object Optiont {
  def main(args: Array[String]): Unit = {
    var map_a: Map[String, Int] = Map("one"->1)
    var map_b :Map[String,Int] = Map()
    var a:Option[Int] =map_a.get("one")
    var b:Option[Int] =map_b.get("one")
    println(a)
    println(b)
    println(a.get)
    println(b.getOrElse(1000))
  }

}
