package sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 查询
 */
object SqlTest3 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sql-test").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")

    val strRdd: RDD[String] = sc.textFile("data.csv")
    val personRdd: RDD[Person] = strRdd.map(it => {
      val arr: Array[String] = it.split(",")
      Person(arr(0), arr(1).trim.toInt)
    })
    import spark.implicits._
    val df: DataFrame = personRdd.toDF()
    //dsl查询
//    df.groupBy("name").sum().show()
//    df.select($"name",$"num"+1).show
    //sql查询
    df.createTempView("person")
//    spark.sql("select name,sum(num) from person group by name").show()


    //全局view
    df.createGlobalTempView("person2")
    spark.newSession().sql("select * from person2").show()
    spark.newSession().sql("select * from person").show()
    spark.stop()

  }

  case class Person(name:String,num:Int)
}
