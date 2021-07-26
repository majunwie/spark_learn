package structedStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SsTest3_file {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("structed-streaming").getOrCreate()
    val ss: SparkContext = spark.sparkContext
    ss.setLogLevel("warn")

//    val str: String = "name num"
//    val fields: Array[StructField] = str.split(" ").map(it => StructField(it, StringType, nullable = false))
//    val structType: StructType = StructType(fields)
    val structType: StructType = new StructType()
      .add(StructField("name", StringType, nullable = false))
      .add(StructField("num", IntegerType, nullable = false))

      spark.readStream
        .option("sep",",")
        .schema(structType)
        .csv("./structedTest")
        .writeStream
        .format("console")
        .outputMode("append")
        .option("truncate","false")
        .start()
        .awaitTermination()
    spark.stop()
  }
}
