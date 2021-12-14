package hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object test1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("hive-test")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","hdfs://server46:8020/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("use test")
    spark.sql("select * from test").show()
    spark.stop()
  }
}
