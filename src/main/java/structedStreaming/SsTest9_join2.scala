package structedStreaming

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * stream join stream
 * 需要watermark和时间范围
 */
object SsTest9_join2 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("structed-streaming").getOrCreate()
    val ss: SparkContext = spark.sparkContext
    ss.setLogLevel("warn")

    import org.apache.spark.sql.functions._
    import spark.implicits._
    //stream1
    val ds1: Dataset[Person1] = spark.readStream
      .format("socket")
      .option("host", "192.168.1.102")
      .option("port", "8888")
      .load()
      .as[String]
      .map(row => {
        val arr: Array[String] = row.split(",")
        Person1(arr(0), Timestamp.valueOf(arr(1)))
      })
      .withWatermark("time1", "10 seconds")
//      .groupBy(
//        window($"time1", "10 seconds") as "window1",
//        $"name1"
//      )
//      .count()
    //stream2
    val ds2: Dataset[Person2] = spark.readStream
      .format("socket")
      .option("host", "192.168.1.102")
      .option("port", "9999")
      .load()
      .as[String]
      .map(row => {
        val arr: Array[String] = row.split(",")
        Person2(arr(0), Timestamp.valueOf(arr(1)))
      })
      .withWatermark("time2", "10 seconds")
//      .groupBy(
//        window($"time2", "10 seconds") as "window2",
//        $"name2"
//      )
//      .count()
    //join
    val result: DataFrame = ds1.join(
      ds2,
      expr(
        """
          |name1=name2 and
          |time1>=time2 and
          |time1<=time2+interval 10 seconds
          |""".stripMargin)
    )//如果是outer join，要多一个参数joinType
    result.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate","false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()
  }
  case class Person1(name1:String,time1:Timestamp)
  case class Person2(name2:String,time2:Timestamp)
}
