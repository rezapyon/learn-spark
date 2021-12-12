package com.github.rezapyon.learn_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class ExamResults {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]").getOrCreate();

//        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

//        dataset = dataset.groupBy("subject").agg(max("score").alias("max_score"),
//                min("score").alias("min_score"), avg("score").alias("avg_score"));

        //pivoting
//        dataset = dataset.groupBy("subject").pivot("year").agg(round(avg("score"), 2).alias("avg_score"),
//                round(stddev("score"), 2).alias("stddev"));

        spark.udf().register("hasPassed", (String grade, String subject) -> {
            if (subject.equals("Biology")) {
                if (grade.startsWith("A")) return true;
                return false;
            }
            return grade.startsWith("A") || grade.startsWith("B") || grade.startsWith("C");
        }, DataTypes.BooleanType);
        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");

//        dataset = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));
        dataset = dataset.withColumn("pass", callUDF("hasPassed", col("grade"), col("subject")));

        dataset.show(100);
    }
}
