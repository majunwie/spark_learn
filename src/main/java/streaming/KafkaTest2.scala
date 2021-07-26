package streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark-kafka  手动提交
 */
object KafkaTest2 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
    val context: SparkContext = new SparkContext(conf)
    val ssc:StreamingContext = new StreamingContext(context, Seconds(5))
    ssc.checkpoint("/ckp")
    ssc.sparkContext.setLogLevel("WARN")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_demo",
      "auto.offset.reset" -> "latest",
//      "auto.commit.interval.ms" -> "1000",
      "enable.auto.commit" -> (false: java.lang.Boolean)//是否自动提交
    )

    val topics = Array("test")
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    dStream.foreachRDD(rdd=>{
      if(!rdd.isEmpty()){
        val ranges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges//获取偏移量
        dStream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)//提交偏移量
        rdd.map(d=>(d.topic(),d.key(),d.value())).foreach(println)
      }
    })
    //消费完之后手动提交
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)
  }
}
