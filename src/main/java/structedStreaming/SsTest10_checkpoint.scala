package structedStreaming

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * checkpoint
 */
object SsTest10_checkpoint {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("structed-streaming").getOrCreate()
    val ss: SparkContext = spark.sparkContext
    ss.setLogLevel("warn")

    import org.apache.spark.sql.functions._
    import spark.implicits._
    //stream1
    val df1 = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 5)
      .load()

//    //1、Continuous trigger with one-second checkpointing interval
//    df1.writeStream
//      .format("console")
//      .outputMode("append")
//      .option("truncate","false")
//      .trigger(Trigger.Continuous("1 seconds"))
//      .start()
//      .awaitTermination()
    //2、
    df1.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation","./check")
      .start()
      .awaitTermination()
  }
}
