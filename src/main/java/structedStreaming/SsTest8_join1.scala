package structedStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * stream  join  static
 */
object SsTest8_join1 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("structed-streaming").getOrCreate()
    val ss: SparkContext = spark.sparkContext
    ss.setLogLevel("warn")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    //读静态数据
    val data: Dataset[String] = spark.read.text("data.txt").as[String]
    val dataMap: Dataset[Person] = data.flatMap(_.split(",")).map(Person(_, 1))
    val dsStatic: DataFrame = dataMap.groupBy("name").agg(sum("num") as "total_num_static")
    //读流数据
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "192.168.1.102")
      .option("port", "8888")
      .load()
    val dsStr: Dataset[String] = df.as[String]
    val dsStream: DataFrame = dsStr
      .flatMap(_.split(","))
      .map(Person(_, 1))
      .groupBy("name").agg(sum("num") as "total_num_stream")
    val result: DataFrame = dsStream.join(dsStatic, Seq("name"), "left_outer")
    result.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate","false")
      .start()
      .awaitTermination()
  }
  case class Person(name:String,num:Integer)
}
