package stream

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 *
 * 滑动窗口
 *
 */
object NcTest4 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rdd-t").setMaster("local[*]")
    val spark = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(spark, Seconds(5))
    ssc.checkpoint("./ckp")

    val data: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.102", 9999)
    val result: DStream[(String, Int)] = data.flatMap(_.split(","))
      .map((_, 1)).reduceByKeyAndWindow(_ + _,Seconds(20),Seconds(5))//必须是微批持续时间的倍数
    result.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)
  }
}
