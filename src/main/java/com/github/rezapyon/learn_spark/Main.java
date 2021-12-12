package com.github.rezapyon.learn_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

//import org.sparkproject.guava.collect.Iterables;
import scala.Tuple2;

//import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args)
    {
//        List<String> inputData =  new ArrayList<>();
//        inputData.add("WARN: Tuesday 4 September 0405");
//        inputData.add("ERROR: Tuesday 4 September 0408");
//        inputData.add("FATAL: Wednesday 5 September 1632");
//        inputData.add("ERROR: Friday 7 September 1854");
//        inputData.add("WARN: Saturday 8 September 1942");

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<String> initialRdd = sc.textFile("s3://");
        JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");

        //JavaRDD<Integer> originalIntegers = sc.parallelize(inputData);
        //JavaRDD<Double> sqrtRdd = originalIntegers.map(Math::sqrt); //originalIntegers.map(value -> Math.sqrt(value));

        //Integer result = originalIntegers.reduce(Integer::sum); //originalIntegers.reduce((value1, value2) -> value1 + value2);

        //// just for testing (you can put this step to hdfs file because usually we use enormous data)
        //// for distributed env (many servers used) but will error in local when your cpu has multiple machine
        // sqrtRdd.foreach(System.out::println); //sqrtRdd.foreach(value -> System.out.println(value));

        //// for local with powerful cpu you can use this instead. this will collect all rdd into the single jre
        //sqrtRdd.collect().forEach(System.out::println);
        //System.out.println(result);

        ////how many elements in sqrtRdd primitive way
        //System.out.println(sqrtRdd.count());

        ////how many elements in sqrtRdd  using just map and reduce
        //JavaRDD<Long> singleIntegerRdd = sqrtRdd.map(value -> 1L);
        //Long count = singleIntegerRdd.reduce(Long::sum); //singleIntegerRdd.reduce((value1, value2) -> value1 + value2);
        //System.out.println(count);

//        JavaRDD<String> originalIntegers = sc.parallelize(inputData);
//        JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalIntegers.map(value -> new Tuple2<>(value, Math.sqrt(value))); //ooriginalIntegers.map(value -> new IntegerWithSquareRoot(value));
//
//        Tuple2<Integer, Double> myValue = new Tuple2<>(9, 3.0);

//        ====================================================================================================
//        JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
//        JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair(rawValue -> {
//            String[] columns = rawValue.split(": ");
//            String level = columns[0];
////            String date = columns[1];
//
////            return new Tuple2<String, Long>(level, date);
//            return new Tuple2<String, Long>(level, 1L);
//        });
//        JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair(rawValue -> new Tuple2<>(rawValue.split(": ")[0], 1L));
//        JavaPairRDD<String, Long> sumRdd = pairRdd.reduceByKey(Long::sum);
//        sumRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

//        sc.parallelize(inputData)
//                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(": ")[0], 1L))
//                .reduceByKey(Long::sum)
//                .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        // group by key example (use this unless there's no other option. (could cause performance problem))
//        sc.parallelize(inputData)
//                .mapToPair(rawValue -> new Tuple2<>(rawValue.split(": ")[0], 1L))
//                .groupByKey()
//                .foreach(tuple -> System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));

        //FLATMAP
//        sc.parallelize(inputData)
//                .flatMap(value -> Arrays.asList(value.split(" ")).iterator())
//                .filter(word -> word.length() > 1)
//                .collect()
//                .forEach(System.out::println);

        // get top 10 keywords that being used in input.txt and not a boring words from boringwors.txt
        JavaRDD<String> lettersOnlyRdd = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter(sentence -> sentence.trim().length() > 0);
        JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
        JavaRDD<String> blanksWordsRemoved = justWords.filter(word -> word.trim().length() > 0);
        JavaRDD<String> justInterestingWords = blanksWordsRemoved.filter(Util::isNotBoring); //justWords.filter(word -> Util.isNotBoring(word));
        JavaPairRDD<String, Long> pairRdd = justInterestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));
        JavaPairRDD<String, Long> totals = pairRdd.reduceByKey(Long::sum);
        JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));
        JavaPairRDD<Long, String> sorted = switched.sortByKey(false);

        // coalesce will give OOM exception in massive data
//        sorted = sorted.coalesce(1);
//        System.out.println(sorted.getNumPartitions());
//        sorted.foreach(element -> System.out.println(element)); // foreach doing sysout parallel in every partition

        List<Tuple2<Long, String>> results = sorted.take(10);
        results.forEach(System.out::println);

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        sc.close();
    }
}
