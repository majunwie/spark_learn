package structedStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * 基本操作  和操作df ds 类似
 */
object SsTest4_opration {

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

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val df: DataFrame = spark.readStream
      .option("sep", ",")
      .schema(structType)
      .csv("./structedTest")
    val dsPerson: Dataset[Person] = df.as[Person]
    val result: DataFrame = dsPerson.groupBy('name).agg(avg($"num") as "avg_val")

    result.writeStream
        .format("console")
        .outputMode("complete")
        .option("truncate","false")
        .start()
        .awaitTermination()
    spark.stop()
  }
  case class Person(name:String,num:Integer)
}
