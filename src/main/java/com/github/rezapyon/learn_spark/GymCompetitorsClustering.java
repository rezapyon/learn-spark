package com.github.rezapyon.learn_spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.checkerframework.checker.units.qual.K;

public class GymCompetitorsClustering {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("Gym Competitors Clustering").master("local[*]").getOrCreate();

        Dataset<Row> csvDataset = spark.read().option("header", true).option("inferSchema", true).csv("src/main/resources/GymCompetition.csv");
        StringIndexer genderIndexer = new StringIndexer();
        genderIndexer.setInputCol("Gender");
        genderIndexer.setOutputCol("GenderIndex");
        csvDataset = genderIndexer.fit(csvDataset).transform(csvDataset);

        OneHotEncoder genderEncoder = new OneHotEncoder();
        genderEncoder.setInputCols(new String[]{"GenderIndex"});
        genderEncoder.setOutputCols(new String[]{"GenderVector"});
        csvDataset = genderEncoder.fit(csvDataset).transform(csvDataset);

        VectorAssembler vectorAssembler = new VectorAssembler();
        Dataset<Row> inputData = vectorAssembler.setInputCols(new String[] {"GenderVector", "Age", "Height", "Weight", "NoOfReps"})
                .setOutputCol("features").transform(csvDataset).select("features");

        KMeans kMeans = new KMeans();

        for (int noOfClusters = 2; noOfClusters <= 8; noOfClusters++){
            kMeans.setK(noOfClusters);

            KMeansModel model = kMeans.fit(inputData);
            Dataset<Row> predictions = model.transform(inputData);
//        predictions.show();

            System.out.println("No of Clusters" + noOfClusters);

//            Vector[] clusterCenters = model.clusterCenters();
//            for (Vector v : clusterCenters) {
//                System.out.println(v);
//            }

            predictions.groupBy("prediction").count().show();

            // deprecated
            //System.out.println("SSE is " + model.computeCost(inputData));

            ClusteringEvaluator evaluator = new ClusteringEvaluator();
            System.out.println("Silhouette with squared euclidean distance is " + evaluator.evaluate(predictions));
        }
    }
}
