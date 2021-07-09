package core

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 持久化
 * 也是转换算子，只有遇到行动算子的时候才会触发
 * 当一个rdd被持久化，下一次在使用同一个rdd的时候就会去memory或者disk中取，提升效率
 * 解决热点数据频繁访问的效率问题
 *
 * python里面做持久化的时候，没有MEMORY_ONLY_SER和MEMORY_AND_DISK_SER这两个选项
 */
object Persist {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val spark: SparkContext = new SparkContext(conf)
    val value: RDD[Int] = spark.parallelize(Array(1, 2, 3, 4, 5)).persist(StorageLevel.MEMORY_ONLY)
    val value2: RDD[Int] = spark.parallelize(Array(1, 2, 3, 4, 5))
    val value3: RDD[Int] = spark.parallelize(Array(1, 2, 3, 4, 5)).persist(StorageLevel.MEMORY_AND_DISK)
    println(value.getStorageLevel)
    println(value2.getStorageLevel)
    println(value3.getStorageLevel)

    //清空缓存
    value.unpersist()
    println(value.getStorageLevel)
  }
}
