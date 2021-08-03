package structedStreaming

import java.sql.Timestamp

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 水印
 */
object SsTest6_window {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("structed-streaming").getOrCreate()
    val ss: SparkContext = spark.sparkContext
    ss.setLogLevel("warn")

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df: DataFrame = spark.readStream.format("socket")
      .option("host", "192.168.1.102")
      .option("port", "9999")
      .load()
    val dsString: Dataset[String] = df.as[String]
    val dsPerson: Dataset[Person] = dsString.map(row => {
      val arr: Array[String] = row.split(",")
      Person(arr(0), Timestamp.valueOf(arr(1)))
    })
    val result: DataFrame = dsPerson.groupBy(
      window($"time", "10 seconds", "5 seconds"),
      $"name"
    ).count()
    result
      .writeStream
      .format("console")
      .outputMode("update")
      .option("truncate","false")
      .start()
      .awaitTermination()
  }
  case class Person(name:String,time:Timestamp)
}
