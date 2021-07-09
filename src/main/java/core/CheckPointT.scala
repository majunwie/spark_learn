package core

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 对于计算复杂、使用多、重要的rdd，可以选择checkpoint
 * 缓存是把数据放在内存或者磁盘上，不能保证数据的安全性
 * checkpoint 把数据放到hdfs中，能保证数据的安全性
 *
 * 数据也可以不放在hdfs上，但是放在hfds上要安全
 *
 * 只有使用了action算子之后，才会触发rdd的checkpoint
 */
object CheckPointT {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val spark: SparkContext = new SparkContext(conf)
    val value: RDD[Int] = spark.parallelize(Array(1, 2, 3, 4, 5))
    val value2: RDD[Int] = spark.parallelize(Array(1, 2, 3, 4, 5))
    spark.setCheckpointDir("./ckp")
    value.checkpoint()
    value2.checkpoint()
    value.foreach(println)
    value2.foreach(println)
    spark.stop()
  }
}
