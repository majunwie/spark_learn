package sql

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * 由文件数据创建df
 */
object SqlTest1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("sql-test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val data1: DataFrame = spark.read.csv("./data.csv")
    data1.show()
    data1.select("_c0").show()
    spark.stop()

  }
}
