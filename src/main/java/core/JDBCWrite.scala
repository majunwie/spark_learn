package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.{Connection, DriverManager, PreparedStatement}


/**
 * jdbc 写
 */
object JDBCWrite {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("persist")
    val spark: SparkContext = new SparkContext(conf)
    val value: RDD[Int] = spark.parallelize(Array(("mjw",1), ("lxq",2)))


    value.foreachPartition(it=>{
      val connection: Connection = DriverManager.getConnection("", "", "")
      val sqlInsert: String = "insert into "
      val ps: PreparedStatement = connection.prepareStatement(sqlInsert)
      it.foreach(t=>{
        val name: String = t._1
        val age: Integer = t._2
        ps.setString(1,name)
        ps.setInt(2,age)
        ps.addBatch()
      })
      ps.executeBatch()
      ps.close()
      connection.close()
    })
    spark.stop()
  }
}
