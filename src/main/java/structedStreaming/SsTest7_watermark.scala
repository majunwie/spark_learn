package structedStreaming

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.Trigger

/**
 * 水印，滚动时窗，触发
 */
object SsTest7_watermark {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("structed-streaming").getOrCreate()
    val ss: SparkContext = spark.sparkContext
    ss.setLogLevel("warn")

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df: DataFrame = spark.readStream.format("socket")
      .option("host", "192.168.1.102")
      .option("port", "6666")
      .load()
    val dsString: Dataset[String] = df.as[String]
    val dsPerson: Dataset[Person] = dsString.map(row => {
      if(null!=row&&row.trim.length>0&&row.contains(",")){
        val arr: Array[String] = row.split(",")
        Person(arr(0), Timestamp.valueOf(arr(1)))
      }else{
        Person("",Timestamp.valueOf("1990-01-01 00:00:00"))
      }
    })
    val result: DataFrame = dsPerson
      .withWatermark("time","10 seconds")
      .groupBy(
      window($"time", "10 seconds","5 seconds"),
      $"name"
    ).count()
    result
      .writeStream
      .format("console")
      .outputMode("update")
      .option("truncate","false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()
  }
  case class Person(name:String,time:Timestamp)
}
