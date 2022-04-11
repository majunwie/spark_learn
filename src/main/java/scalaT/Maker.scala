package scalaT

class Maker private(val color:String){
  println("创建"+this)
  override def toString: String =  "颜色标记"+color
}

object Maker{
  def main(args: Array[String]): Unit = {
    val maker = new Maker("red")
    println(maker.toString)
  }
}
