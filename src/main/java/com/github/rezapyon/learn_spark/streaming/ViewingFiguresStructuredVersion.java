package com.github.rezapyon.learn_spark.streaming;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.streaming.Durations;

import java.util.concurrent.TimeoutException;

public class ViewingFiguresStructuredVersion {
    public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);

        SparkSession session = SparkSession.builder().appName("structuredViewingReport").master("local[*]").getOrCreate();

        Dataset<Row> df = session.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "viewrecords")
                .option("value.deserializer", String.valueOf(StringDeserializer.class))
                .load();

        df.createOrReplaceTempView("viewing_figures");

        Dataset<Row> results = session.sql("SELECT window, cast(value as string) as course_name, sum(5) as seconds_watched from viewing_figures group by window(timestamp, '1 minute'), 2");
        StreamingQuery query = results.writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                .option("truncate", false)
                .option("numRows", 50)
//                .trigger(Trigger.ProcessingTime(30000))
                .start();

        query.awaitTermination();

    }

}
