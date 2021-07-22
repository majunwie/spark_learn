
package sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession, functions}

/**
 * 平均分 top5
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

    df.createTempView("film")
    val sql1: String =
      """
        |select
        |film_name,
        |avg(score) as avg_score
        |from film
        |where score>1
        |group by film_name
        |order by avg_score desc
        |limit 5
        |""".stripMargin
    spark.sql(sql1).show()

    val rows2: Array[Row] = df.groupBy("film_name").agg(avg("score") as "avg_score", count("film_name") as "num")
      .filter($"num" > 1).orderBy('avg_score.desc).take(5)
    rows2.foreach(println)

    val sql2:String =
      """
        |select
        |film_name,
        |avg(score) as avg_score,
        |count(1) as num
        |from film
        |group by film_name
        |having num>1
        |order by avg_score desc
        |limit 5
        |""".stripMargin
    spark.sql(sql2).show()
    spark.stop()

  }
  case class Film(film_name:String,score:Integer)
}
