package ml

import breeze.linalg.Matrix
import org.apache.spark.ml.linalg
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.sql.{Row, SparkSession}

object correlation {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("mlt").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )
    val result: Seq[Tuple1[linalg.Vector]] = data.map(Tuple1.apply)
    result.foreach(println)
  }
}
