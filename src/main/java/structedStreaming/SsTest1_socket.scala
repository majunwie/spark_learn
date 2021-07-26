package structedStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * rate 测试时使用
 * 随机生成数据
 */
object SsTest1_socket {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("structed-streaming").getOrCreate()
    val ss: SparkContext = spark.sparkContext
    ss.setLogLevel("warn")
    spark.readStream
      .format("rate")
      .option("rowsPerSecond ", "5")
      .option("rampUpTime ", "1")
      .option("numPartitions  ", "2")
      .load()
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate","false")
      .start()
      .awaitTermination()
    spark.stop()
  }
}
