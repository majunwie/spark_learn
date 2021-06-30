package sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SqlTest1 {
    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder().master("local").appName("sql").config("spark.some.config.option", "some-value").getOrCreate();
        Dataset<Row> ds = ss.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("data.csv");
        ds.select("name").show();
    }
}
