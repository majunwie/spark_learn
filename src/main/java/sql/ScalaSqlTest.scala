package sql

import org.apache.spark.sql.SparkSession

object ScalaSqlTest {
  def main(args: Array[String]): Unit = {
    print(1)
    val sparkSession = SparkSession.builder().appName("ssql").master("local").config("spark.some.config.option", "some-value").getOrCreate()
    //导入数据
    val df = sparkSession.read.csv("data.csv")
    df.printSchema()
    df.show()
  }
}
