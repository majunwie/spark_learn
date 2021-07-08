package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * rdd练习--行动算子
 * @author mjw
 * 2021-07-08
 */
object RddAction {
  def main(args: Array[String]): Unit = {
    //环境
    val conf = new SparkConf().setAppName("rdd-t").setMaster("local[*]")
    val spark = new SparkContext(conf)
    spark.setLogLevel("WARN")
    //reduce
    val data: RDD[Int] = spark.parallelize(Array(3, 5, 2, 4, 1))
//    val result: Int = data.reduce(_ + _)
//    println(result)
//    //collect
//    val ints: Array[Int] = data.collect()
//    ints.foreach(it=>println(it))
//    //count
//    val num: Long = data.count()
//    println(num)
//    //first
//    println("--")
//    val firstOne: Int = data.first()
//    println(firstOne)
//    //take
//    println("--")
//    val firstN: Array[Int] = data.take(3)
//    firstN.foreach(it=>println(it))
//    //takeSample
//    println("--")
//    val takeR: Array[Int] = data.takeSample(false, 2, 1)
//    takeR.foreach(it=>println(it))
//    //takeOrdered 按顺序取前n个
//    val ints1 = data.takeOrdered(3)
//    ints1.foreach(it=>println(it))
//    //自定义顺序
//    val reverse: Ordering[Int] = implicitly[Ordering[Int]].reverse
//    val ints2 = data.takeOrdered(3)(reverse)
//    ints2.foreach(it=>println(it))
//    //top
//    val top3: Array[Int] = data.top(3)
//    top3.foreach(it=>println(it))
    //countByKey
    val intToLong = data.map((_, 1)).countByKey()
    intToLong.foreach(it=>println(it))
//    data.saveAsTextFile("arr.txt")
    spark.stop()
  }
}
