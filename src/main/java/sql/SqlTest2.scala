package sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * rdd创建df、ds
 * df和ds互转
 */
object SqlTest2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("sql-test").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
//    println("++++++RDD转为DataFrame方法1++++++")
//    //rdd
//    val rdd: RDD[String] = sc.textFile("data.csv")
//    val personRdd: RDD[Person] = rdd.map(it => {
//      val arr: Array[String] = it.split(",")
//      Person(arr(0), Integer.valueOf(arr(1)))
//    })
//    import spark.implicits._
//    val df: DataFrame = personRdd.toDF()
//    df.show()
//    df.select("name").show()
//    println("++++++RDD转为DataSets++++++")
//    val ds: Dataset[Person] = personRdd.toDS()
//    ds.show()
//    ds.select("num").show()
//    println("++++++DataFrame转为DataSets++++++")
//    val df2ds: Dataset[Person] = df.as[Person]
//    df2ds.show()
//    println("++++++DataFrame转为DataSets++++++")
//    val ds2df: DataFrame = ds.toDF()
//    ds2df.show()
    println("++++++RDD转为DataFrame方法2++++++")
    val rdd2:RDD[String] = sc.textFile("data.csv")
    val rowRdd2: RDD[(String, Int)] = rdd2.map(it => {
      val arr: Array[String] = it.split(",")
      (arr(0), arr(1).trim.toInt)
    })
    import spark.implicits._
    rowRdd2.toDF("name","age").show()
//    println("++++++RDD转为DataFrame方法3++++++")
//    import spark.implicits._
//    val rdd3:RDD[String] = sc.textFile("data.csv")
//    val schemaStr: String = "name age"
//    val fields: Array[StructField] = schemaStr.split(" ").map(it => StructField(it, StringType, nullable = true))
//    val structType: StructType = StructType(fields)
//    val rowRdd: RDD[Row] = rdd3.map(_.split(",")).map(it => Row(it(0), it(1).trim))
//    val df2: DataFrame = spark.createDataFrame(rowRdd, structType)
//    df2.show()
//    println("++++++seq  to  ds++++++")
//    val seq2ds: Dataset[Person] = Seq(Person("lll", 10)).toDS()
//    seq2ds.show()

    //df和ds转为rdd：df.rdd    ds.rdd
      //关闭资源
    spark.stop()
  }

  case class Person(name:String,num:Int)
}
