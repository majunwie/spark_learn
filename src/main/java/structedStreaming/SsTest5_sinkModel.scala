package structedStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 基本操作  和操作df ds 类似
 */
object SsTest5_sinkModel {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("structed-streaming").getOrCreate()
    val ss: SparkContext = spark.sparkContext
    ss.setLogLevel("warn")

    val structType: StructType = new StructType()
      .add(StructField("name", StringType, nullable = false))
      .add(StructField("num", IntegerType, nullable = false))

    import org.apache.spark.sql.functions._
    import spark.implicits._
    val df: DataFrame = spark.readStream
      .option("sep", ",")
      .schema(structType)
      .csv("./structedTest")
    val dsPerson: Dataset[Person] = df.as[Person]
    val result: DataFrame = dsPerson.groupBy('name).agg(avg($"num") as "avg_val")

    result.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate","false")
        .start()
        .awaitTermination()
    spark.stop()
  }
  case class Person(name:String,num:Integer)
}
