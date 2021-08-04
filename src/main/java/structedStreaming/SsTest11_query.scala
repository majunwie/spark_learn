package structedStreaming

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryListener}

/**
 * query
 */
object SsTest11_query {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("structed-streaming").getOrCreate()
    val ss: SparkContext = spark.sparkContext
    ss.setLogLevel("warn")
    spark.readStream
      .format("socket")
      .option("host", "192.168.1.102")
      .option("port", "8888")
      .load()
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
        println("query started"+event.id)
      }
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        println("query processing"+event.progress)
      }

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        println("query end"+event.id)
      }
    })
    spark.streams.awaitAnyTermination()

  }
}
