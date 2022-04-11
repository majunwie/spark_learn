package scalaT

object FunctionT {
  def main(args: Array[String]): Unit = {
    var a = 100
    //闭包
    def f_a = (x:Int)=>x+a
    println(f_a(100))
    //柯里化
    def f_kl = (x:Int)=>(y:Int)=>x+y
    println(f_kl(10)(20))

  }


}
