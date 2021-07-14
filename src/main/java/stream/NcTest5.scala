package stream

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 *
 * top5  transform 直接操作rdd
 *
 */
object NcTest5 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rdd-t").setMaster("local[*]")
    val spark = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(spark, Seconds(5))
    ssc.checkpoint("./ckp")
    ssc.sparkContext.setLogLevel("WARN")

    val data: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.102", 9999)
    val resultDs: DStream[(String, Int)] = data.flatMap(_.split(",")).map((_, 1))
    resultDs.transform(rdd=>{
      val sortedRdd: RDD[(String, Int)] = rdd.sortBy(_._2,false)
      val top5: Array[(String, Int)] = sortedRdd.take(5)
      top5.foreach(println)
      sortedRdd
    })
    resultDs.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)
  }
}
