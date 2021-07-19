package sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * rdd创建df、ds
 * df和ds互转
 */
object SqlTest2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("sql-test").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    //rdd
    val rdd: RDD[String] = sc.textFile("data.csv")
    val personRdd: RDD[Person] = rdd.map(it => {
      val arr: Array[String] = it.split(",")
      Person(arr(0), Integer.valueOf(arr(1)))
    })
    import spark.implicits._
    println("++++++RDD转为DataFrame++++++")
    val df: DataFrame = personRdd.toDF()
    df.show()
    df.select("name").show()
    println("++++++RDD转为DataSets++++++")
    val ds: Dataset[Person] = personRdd.toDS()
    ds.show()
    ds.select("num").show()
    println("++++++DataFrame转为DataSets++++++")
    val df2ds: Dataset[Person] = df.as[Person]
    df2ds.show()
    println("++++++DataFrame转为DataSets++++++")
    val ds2df: DataFrame = ds.toDF()
    ds2df.show()
    //关闭资源
    spark.stop()
  }

  case class Person(name:String,num:Int)
}
