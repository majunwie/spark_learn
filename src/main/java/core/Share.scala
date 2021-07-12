package core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 共享变量
 */
object Share {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val spark: SparkContext = new SparkContext(conf)

    //累加器
    val charNumAccumulator: LongAccumulator = spark.longAccumulator("charNum")
    //广播变量
    val charList = List(".", "!", ",", "$", "%", "&", "*", "@", "#", "^")
    val broadList: Broadcast[List[String]] = spark.broadcast(charList)

    val lines: RDD[String] = spark.textFile("share.txt")
    val result: Array[(Int,String)] = lines.flatMap(_.split("\\s+"))
      .filter(it => {
        val list: List[String] = broadList.value
        if (list.contains(it)) {
          charNumAccumulator.add(1)
          false
        } else {
          true
        }
      }).map((_, 1)).reduceByKey(_ + _)
      .map(_.swap).sortByKey(false).take(5)
    result.foreach(println)
    println(charNumAccumulator.value)
  }
}
