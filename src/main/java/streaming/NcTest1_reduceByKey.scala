package streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}


/**
 * 统计当前批次
 * reduceByKey
 */
object NcTest1_reduceByKey {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rdd-t").setMaster("local[*]")
    val spark = new SparkContext(conf)
    spark.setLogLevel("WARN")
    val ssc: StreamingContext = new StreamingContext(spark, Seconds(5))

    val data: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.102", 9999)
    val result: DStream[(String, Int)] = data.flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _)
    result.print()

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(true,true)
  }
}
