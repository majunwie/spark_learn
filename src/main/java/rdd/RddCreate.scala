package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * rdd创建
 * @author mjw
 * 2021-07-08
 */
object RddCreate {
  def main(args: Array[String]): Unit = {
    //环境
    val conf = new SparkConf().setAppName("rdd-t").setMaster("local[*]")
    val spark = new SparkContext(conf)
    spark.setLogLevel("WARN")
    //从集合中创建rdd
    val data = Array(1,2,3,4,5,6)
    val arrRdd = spark.parallelize(data,3)
    var value: RDD[Int] = spark.makeRDD(data)
    print(arrRdd.getNumPartitions)
    //从文件中创建
    val lines = spark.textFile("data.txt",1)
    val lines2 = spark.textFile("data2.txt",1)
    lines.map(it => {
      val arr = it.split(",")
      (arr(0), Integer.valueOf(arr(1)))
    }).reduceByKey(_ + _).foreach(it=>print(it))
    lines.flatMap(_.split(",")).mapPartitions(it=>{
      it.map((_,1))
    }).aggregateByKey(0)(_+_,_*_).foreach(it=>println(it))
  }
}
