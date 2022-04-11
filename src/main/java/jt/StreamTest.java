package jt;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;

public class StreamTest {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("stream-test").setMaster("spark://server46:7077");
        SparkContext sparkContext = new SparkContext(sparkConf);
        sparkContext.setLogLevel("WARN");
        StreamingContext ssc = new StreamingContext(sparkContext, new Duration(5000));
        ReceiverInputDStream<String> data = ssc.socketTextStream("192.168.1.107", 9999, StorageLevel.MEMORY_AND_DISK_SER_2());
        data.print();
        ssc.start();
        ssc.awaitTerminationOrTimeout(100000);
        ssc.stop(true,true);
    }
}
