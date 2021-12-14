package ml

import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.ml.stat.{ChiSquareTest, Correlation}
import org.apache.spark.sql.{Row, SparkSession}

object correlation {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("mlt").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    val data = Seq(
      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0))
    )
    import spark.implicits._
    val df = data.toDF("label", "features")
    val chi = ChiSquareTest.test(df, "features", "label").head
    df.show()
    println(chi)
    spark.stop()
  }
}
