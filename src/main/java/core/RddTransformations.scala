package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 * rdd练习--转换算子
 * @author mjw
 * 2021-07-08
 */
object RddTransformations {
  def main(args: Array[String]): Unit = {
    //环境
    val conf = new SparkConf().setAppName("rdd-t").setMaster("local[*]")
    val spark = new SparkContext(conf)
    spark.setLogLevel("WARN")
    //从文件中创建
    val lines = spark.textFile("data.txt",1)
    val lines2 = spark.textFile("data2.txt",1)
    //flatmap:将每行数据按“,”分割，得到一个集合
    val flatmapRdd: RDD[String] = lines.flatMap(_.split(","))
//    val flatmapRdd2: RDD[String] = lines2.flatMap(_.split(","))
    //    flatmapRdd.foreach(it=>println(it))
        //filter:过滤掉没有m的字符
    //    val filterRdd = flatmapRdd.filter(_.contains("m"))
    //    filterRdd.foreach(it=>println(it))
        //map:转为(word,1)
        //与mapPartitions相比，函数是作用在每行数据上
    //    val mapRdd: RDD[(String, Int)] = flatmapRdd.map((_, 1))
    //    val mapRdd2: RDD[(String, Int)] = flatmapRdd2.map((_, 1))
    //    mapRdd.foreach(it=>println(it))
    //    println("--")
    //    mapRdd2.foreach(it=>println(it))
    //    println("--")
            //mapPartitions
            //与map相比，函数是作用在分区上
        //    val mapPartaRdd: RDD[(String, Int)] = flatmapRdd.mapPartitions(it => {
        //      it.map((_, 1))
        //    })
            //    mapPartaRdd.foreach(it=>println(it))
                //sample:采样(是否有放回，采样率，种子)
            //    println("--")
            //    val sampleRdd: RDD[String] = flatmapRdd.sample(false, 0.5)
            //    sampleRdd.foreach(it=>println(it))
                //union
            //    val value: RDD[String] = flatmapRdd.union(flatmapRdd2)
            //    value.foreach(it=>println(it))
                //intersection
            //    val interactionRdd: RDD[String] = flatmapRdd.intersection(flatmapRdd2)
            //    interactionRdd.foreach(it=>println(it))
                //distinct
            //    val distinctRdd: RDD[String] = flatmapRdd.distinct()
            //    distinctRdd.foreach(it=>println(it))
            //groupByKey
        //    val groupByKeyRdd: RDD[(String, Iterable[Int])] = mapPartaRdd.groupByKey()
        //    groupByKeyRdd.foreach(it=>println(it))
                //reduceByKey
        //        val reduceByKeyRdd: RDD[(String, Int)] = mapPartaRdd.reduceByKey(_ + _)
        //        reduceByKeyRdd.foreach(it=>println(it))
            //aggregateByKey
        //        val aggregateByKeyRdd: RDD[(String, Int)] = mapPartaRdd.aggregateByKey(10)(_ + _, _ + _)
        //        aggregateByKeyRdd.foreach(it=>println(it))
            //sortByKey
        //    val sortByKeyRdd: RDD[(String, Int)] = mapPartaRdd.sortByKey()
        //    sortByKeyRdd.foreach(it=>print(it))
        //join:返回的Some是什么
    //    val joinRdd: RDD[(String, (Int, Int))] = mapRdd.join(mapRdd2)
    //    val leftJoinRdd: RDD[(String, (Int, Option[Int]))] = mapRdd.leftOuterJoin(mapRdd2)
    //    val rightJoinRdd: RDD[(String, (Option[Int], Int))] = mapRdd.rightOuterJoin(mapRdd2)
    //    joinRdd.foreach(it=>print(it))
    //    println("--")
    //    leftJoinRdd.foreach(it=>print(it))
    //    println("--")
    //    rightJoinRdd.foreach(it=>print(it))
        //cogroup  groupWith
    //    val cogroupRdd: RDD[(String, (Iterable[Int], Iterable[Int]))] = mapRdd.cogroup(mapRdd2)
    //    val groupwithRdd: RDD[(String, (Iterable[Int], Iterable[Int]))] = mapRdd.groupWith(mapRdd2)
    //    cogroupRdd.foreach(it=>print(it))
    //    println("--")
    //    groupwithRdd.foreach(it=>print(it))
    //cartesian
//        val cartesianRdd:RDD[(String, String)] = flatmapRdd.cartesian(flatmapRdd2)
//        cartesianRdd.foreach(it=>println(it))

    //重新设置分区 repartition  coalesce
//    val repartitionRdd: RDD[String] = lines.repartition(2)
//    println(repartitionRdd.getNumPartitions)
//    //coalesce默认只能减不能加，因为默认是不shuffle
//    val coalesceRdd1: RDD[String] = repartitionRdd.coalesce(3)
//    println(coalesceRdd1.getNumPartitions)
//    val coalesceRdd2: RDD[String] = coalesceRdd1.coalesce(1)
//    println(coalesceRdd2.getNumPartitions)
//    //coalesce  shuffle设置为true，则可以增加分区数
//    val coalescePlus: RDD[String] = coalesceRdd2.coalesce(4, shuffle = true)
//    println(coalescePlus.getNumPartitions)

    //pipe 每个分区调用shell脚本
//    flatmapRdd.pipe("pipe.sh")

  }
}
