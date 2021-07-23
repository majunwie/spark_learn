package sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 查询
 */
object SqlTest7_udf {
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
    df.createTempView("person")
    //udf
    spark.udf.register("myf",(num:Int)=>num*1000)
    spark.sql("select myf(num) from person").show()
    //
    import org.apache.spark.sql.functions._
    val myfc: UserDefinedFunction = udf((num: Int) => {
      num * 1000
    })
    df.select(myfc($"num")).show()
    spark.stop()

  }

  case class Person(name:String,num:Int)
}
