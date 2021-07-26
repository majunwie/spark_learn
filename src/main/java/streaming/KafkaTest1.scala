package streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
 * spark-kafka  自动提交
 */
object KafkaTest1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kafka")
    val context: SparkContext = new SparkContext(conf)
    val ssc:StreamingContext = new StreamingContext(context, Seconds(5))
//    ssc.checkpoint("/ckp")
    ssc.sparkContext.setLogLevel("WARN")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_demo",
      "auto.offset.reset" -> "latest",
      "auto.commit.interval.ms" -> "1000",
      "enable.auto.commit" -> (true: java.lang.Boolean)//是否自动提交
    )

    val topics = Array("test")
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    dStream.map(d=>(d.topic(),d.key(),d.value())).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop(true,true)
  }

}
