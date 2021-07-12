package core

import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.JdbcRDD.ConnectionFactory
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}


/**
 * jdbc 读
 */
object JDBCRead {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val spark: SparkContext = new SparkContext(conf)
    val getConnection = () => DriverManager.getConnection("","","")
    val row = (r:ResultSet) =>{
      val name: String = r.getString("name")
      val age: Int = r.getInt("age")
      (name,age)
    }
    val dataRdd = new JdbcRDD(
      spark,
      getConnection,
      "",
      3,
      6,
      1,
      row
    )
    dataRdd.foreach(println)
    spark.stop()
  }
}
