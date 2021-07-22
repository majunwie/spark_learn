
package sql

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * wordcount
 */
object SqlTest5_rw {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sql-test").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")


    import spark.implicits._
    val ds: Dataset[String] = spark.read.textFile("data.txt")
    val words: Dataset[String] = ds.flatMap(_.split(","))
    val value: Dataset[Row] = words.groupBy('value).count().orderBy('count.desc)
    val value1: Dataset[String] = value.map(row => {
      row(0)+","+row(1)
    })
//    //存入文件txt
//    value1.write.text("data/spark_test.txt")
//    val df: DataFrame = spark.read.text("data/spark_test.txt")
//    df.show()
    //
//    value1.write.mode(SaveMode.Overwrite).csv("data/value1.csv")
//    value1.write.mode(SaveMode.Overwrite).json("data/value1.json")
    value1.coalesce(1).write.mode(SaveMode.Overwrite).json("data/value1.json1")//只有一个分区
//    //存入pg
//    val properties = new Properties()
//    properties.setProperty("user","postgres")
//    properties.setProperty("password","longruan2018")
//    properties.setProperty("driver","org.postgresql.Driver")
//    value.write.mode(SaveMode.Overwrite).jdbc("jdbc:postgresql://192.168.1.25:54321/ark","model.spark_test",properties)
    //
    spark.stop()

  }
}
