package structedStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SsTest2_Rate {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("structed-streaming").getOrCreate()
    val ss: SparkContext = spark.sparkContext
    ss.setLogLevel("warn")
    import spark.implicits._
    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "192.168.1.102")
      .option("port", "8888")
      .load()
    val ds: Dataset[String] = df.as[String]
    val dsRow: Dataset[Row] = ds.flatMap(_.split(",")).groupBy("value").count().orderBy('count.desc)
    dsRow.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
    spark.stop()
  }
}
