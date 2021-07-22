
package sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * 平均分 top10
 */
object SqlTest6_avgtop {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sql-test").master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("warn")

    import spark.implicits._
    val filmRdd: RDD[String] = sc.textFile("film.txt")
    val mapRdd: RDD[(String, Integer)] = filmRdd.map(it => {
      val arr: Array[String] = it.split(",")
      (arr(0), Integer.valueOf(arr(1)))
    })
    val df: DataFrame = mapRdd.toDF("film_name", "score")
    val ds: Dataset[Film] = df.as[Film]
    val rows: Array[Row] = ds.filter($"score" > 1)
      .groupBy("film_name")
      .agg(avg("score").as("avg_score"))
      .orderBy('avg_score.desc).take(5)
    rows.foreach(println)
    spark.stop()

  }
  case class Film(film_name:String,score:Integer)
}
