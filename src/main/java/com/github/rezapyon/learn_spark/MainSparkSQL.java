package com.github.rezapyon.learn_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.*;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class MainSparkSQL {
    public static void main(String[] args)
    {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        // JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();
//        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
//        dataset.show();
//
//        long numberOfRows = dataset.count();
//        System.out.println(numberOfRows);
//
//        Row firstRow = dataset.first();
//        String subject = firstRow.getAs("subject").toString();
//        System.out.println(subject);
//
//        int year = Integer.parseInt(firstRow.getAs("year"));
//        System.out.println("The year was " + year);

        //Dataset<Row> modernArtResults  = dataset.filter("subject = 'Modern Art' AND year >= 2007");
//        Dataset<Row> modernArtResults  = dataset.filter((FilterFunction<Row>) row -> row.getAs("subject").equals("Modern Art")
//                && Integer.parseInt(row.getAs("year")) >= 2007);

//        Dataset<Row> modernArtResults = dataset.filter(col("subject").equalTo("Modern Art")
//                                                                        .and(col("year").geq(2007)));

//        dataset.createOrReplaceTempView("my_students_table");
//        Dataset<Row> results =  spark.sql("SELECT DISTINCT(year) FROM my_students_table ORDER BY 1");
//        results.show();

//        List<Row> inMemory = new ArrayList<Row>();
//        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
//        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
//        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
//        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
//        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));
//
//        StructField[] fields = new StructField[]{
//                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
//                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
//        };
//        StructType schema = new StructType(fields);
//        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);
//
        // improve perfomance of sparksql by sql
        spark.conf().set("spark.sql.shuffle.partitions", "12");

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");

//        SimpleDateFormat input = new SimpleDateFormat("MMMM");
//        SimpleDateFormat output = new SimpleDateFormat("M");

//        spark.udf().register("monthNum", (String month) -> {
//           java.util.Date inputDate = input.parse(month);
//           return Integer.parseInt(output.format(inputDate));
//        }, DataTypes.IntegerType);
//
        dataset.createOrReplaceTempView("logging_table");
//        Dataset<Row> result = spark.sql("SELECT level, date_format(datetime, 'MMMM') as month, count(1) as total"
//                + " FROM logging_table GROUP BY 1, 2 ORDER BY monthNum(month), 1");

        Dataset<Row> result = spark.sql("SELECT level, date_format(datetime, 'MMMM') as month, count(1) as total,"
                + " date_format(datetime, 'M') as monthnum FROM logging_table GROUP BY 1, 2, 4 ORDER BY 4, 1");

        result = result.drop("monthNum");
        result.show(100);

//        dataset = dataset.select(col("level"),date_format(col("datetime"), "MMMM").
//                alias("month"), date_format(col("datetime"), "M").alias("monthNum").cast(DataTypes.IntegerType));
//        dataset = dataset.groupBy(col("level"), col("month"), col("monthNum")).count();
//        dataset = dataset.orderBy("monthNum", "level");

        result.explain();

//        Object[] months = new Object[] {"January", "February", "March", "April", "May", "June", "July", "August", "September", "Oktober", "November", "December"};
//        List<Object> columns = Arrays.asList(months);
//        dataset = dataset.groupBy("level").pivot("month", columns).count().na().fill(0);

        // SparkSQL Performance dig dive

//        dataset = dataset.groupBy("level", "month", "monthnum").count().as("total").orderBy("monthnum");
//        dataset = dataset.drop("monthnum");

//        dataset.show(100);

//        Scanner scanner = new Scanner(System.in);
//        scanner.nextLine();

        spark.close();

    }
}
