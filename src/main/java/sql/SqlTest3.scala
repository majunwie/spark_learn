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
    //查询num大于30的分组结果
//    df.filter($"num" > 20).groupBy("name").avg().show()
//    df.where($"num" >20).groupBy("name").avg().show()
//    df.groupBy("name").pivot("num").count().show()
    //sql查询
    df.createTempView("person")
//    spark.sql("select name,sum(num) from person group by name").show()
//    //全局view
//    df.createGlobalTempView("person2")
//    spark.newSession().sql("select * from global_temp.person2").show()
//    spark.newSession().sql("select * from person").show()


    //udf
    spark.udf.register("myf",(num:Int)=>num*1000)
    spark.sql("select myf(num) from person").show()

    spark.stop()

  }

  case class Person(name:String,num:Int)
}
