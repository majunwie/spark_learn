package streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * 状态管理，可以将不同批次数据进行累加
 * updateStateByKey
 * 状态恢复：重启之后恢复之前的计算结果
 *
 */
object NcTest3_getOrCreate {

  def createFunc(): StreamingContext = {
    val conf = new SparkConf().setAppName("rdd-t").setMaster("local[*]")
    val spark = new SparkContext(conf)
    val ssc: StreamingContext = new StreamingContext(spark, Seconds(5))
    ssc.checkpoint("./ckp")
    //    updateFunc: (Seq[V], Option[S]) => Option[S]
    val updateFunc = (thisValue:Seq[Int], hisValue:Option[Int])=>{
      if(thisValue.size>0){
        val sum = thisValue.sum+hisValue.getOrElse(0)
        Some(sum)
      }else{
        hisValue
      }
    }
    val data: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.102", 9999)
    val result: DStream[(String, Int)] = data.flatMap(_.split(","))
      .map((_, 1))
      .updateStateByKey(updateFunc)
    result.print()
    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc: StreamingContext = StreamingContext.getOrCreate("./ckp", createFunc _)
    ssc.sparkContext.setLogLevel("WARN")
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)
  }
}
