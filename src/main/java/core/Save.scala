package core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 保存数据
 */
object Save {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val spark: SparkContext = new SparkContext(conf)

    val data: RDD[Int] = spark.parallelize(Array(1, 2, 4))
    data.saveAsTextFile("./data/result1")
//    data.saveAsObjectFile("./data/result2")
    spark.stop()
  }
}
