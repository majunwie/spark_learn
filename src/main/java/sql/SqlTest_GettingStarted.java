package sql;

import model.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.janino.Java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SqlTest_GettingStarted {
    public static void main(String[] args) throws AnalysisException {
        SparkSession ss = SparkSession.builder().master("local").appName("sql").config("spark.some.config.option", "some-value").getOrCreate();
        ss.sparkContext().setLogLevel("WARN");
                Dataset<Row> ds = ss.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("data.csv");
        //查询
        ds.select("name").show();
//        ds.select("name","num").show();
//        ds.groupBy("name").avg().show();
//        ds.filter(col("num").gt(29)).show();
//        ds.select(col("num").plus(100)).show();
//        //tempView，session内使用
//        ds.createOrReplaceTempView("pp");
//        ss.sql("select * from pp").show();
//        //全局的tempView，垮session使用
//        ds.createGlobalTempView("pp");
//        ss.newSession().sql("select * from global_temp.pp").show();
//        //由集合创建dataset
//        Person person = new Person();
//        person.setName("my");
//        person.setNum(30);
//        Encoder<Person> personEncoder = Encoders.bean(Person.class);
//        Dataset<Person> dsPerson = ss.createDataset(
//                Collections.singletonList(person), personEncoder
//        );
//        dsPerson.show();
//        //
//        Encoder<Long> longEncoder = Encoders.LONG();
//        Dataset<Long> longDataset = ss.createDataset(Arrays.asList(1L,2L,3L),longEncoder);
//        longDataset.show();
//        Dataset<Long> longMapDataSet = longDataset.map(
//                (MapFunction<Long, Long>) value->value+1L,longEncoder
//        );
//        longMapDataSet.show();
//        //由RDD创建Datasets和 DataFrame
//        //1、定义了javaBean
//        JavaRDD<Person> personJavaRDD = ss.read().textFile("data.txt").javaRDD()
//                .map(line->{
//                    String[] arr = line.split(",");
//                    Person person =new Person();
//                    person.setName(arr[0]);
//                    person.setNum(Integer.parseInt(arr[1]));
//                    return person;
//                });
//        //转DataFrame
//        Dataset<Row> personDF = ss.createDataFrame(personJavaRDD,Person.class);
//        personDF.show();
//        //转Datasets
//        Encoder<Person> personEncoder = Encoders.bean(Person.class);
//        Dataset<Person> personDS = ss.createDataset(personJavaRDD.rdd(),personEncoder);
//        personDS.show();
//        //2、没有定义javaBean
//        JavaRDD<String> stringRdd = ss.read().textFile("data.txt").javaRDD();
//        String schemaStr = "name,num";
//        List<StructField> fieldList = new ArrayList<>();
//        for (String s : schemaStr.split(",")) {
//            StructField structField = DataTypes.createStructField(s,DataTypes.StringType,true);
//            fieldList.add(structField);
//        }
//        StructType structType = DataTypes.createStructType(fieldList);
//        JavaRDD<Row> rowJavaRDD = stringRdd.map(it->{
//            String[] arr = it.split(",");
//            return RowFactory.create(arr[0],arr[1]);
//        });
//        Dataset<Row> ds = ss.createDataFrame(rowJavaRDD,structType);
//        ds.show();
//        ss.stop();



    }
}
