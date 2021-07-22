package sql

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * wordcount
 */
object SqlTest4_wordcount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sql-test").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")
    import spark.implicits._
    val ds: Dataset[String] = spark.read.textFile("data.txt")
    val words: Dataset[String] = ds.flatMap(_.split(","))
//    words.createTempView("words")
//    spark.sql("select value,count(1) as num from words group by value order by num desc").show()
    words.groupBy('value).count().orderBy('count.desc).show()
    spark.stop()

  }
}
